import azure.functions as func
import logging
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Dict, List, Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.ai.contentunderstanding import ContentUnderstandingClient
from PyPDF2 import PdfReader, PdfWriter

from azure.ai.contentunderstanding.models import DocumentContent


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

_credential: Optional[DefaultAzureCredential] = None


def get_default_credential() -> DefaultAzureCredential:
    """Create (or reuse) a DefaultAzureCredential for managed identity auth."""
    global _credential  # type: ignore[global-statement]
    if _credential is None:
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
        _credential = DefaultAzureCredential(managed_identity_client_id=client_id) if client_id else DefaultAzureCredential()
    return _credential


def get_blob_content(blob_name: str) -> bytes:
    """Download PDF document from Azure Blob Storage."""
    account_url = os.environ.get("BLOB_ACCOUNT_URL")
    if not account_url:
        raise ValueError("BLOB_ACCOUNT_URL must be configured with the storage account blob endpoint.")

    container_name = os.environ.get("BLOB_CONTAINER_NAME", "documents")

    blob_service_client = BlobServiceClient(account_url=account_url, credential=get_default_credential())
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    return blob_client.download_blob().readall()


def split_pdf_into_pages(document_bytes: bytes) -> List[bytes]:
    """Split a PDF into individual page PDFs as bytes."""
    reader = PdfReader(BytesIO(document_bytes))
    page_documents: List[bytes] = []

    for page in reader.pages:
        writer = PdfWriter()
        writer.add_page(page)

        buffer = BytesIO()
        writer.write(buffer)
        page_documents.append(buffer.getvalue())
        buffer.close()

    return page_documents or [document_bytes]


def extract_all_field_values(content: DocumentContent) -> Dict[str, object]:
    fields = getattr(content, "fields", {}) or {}
    return {name: getattr(field, "value", None) for name, field in fields.items()}


def get_field_candidates() -> List[str]:
    raw = os.environ.get("FIELD_CANDIDATES")
    if raw:
        parts = [candidate.strip() for candidate in raw.split(",") if candidate.strip()]
        if parts:
            return parts
    return ["TT"]


def resolve_field_value(fields: Dict[str, object], candidates: List[str]):
    for candidate in candidates:
        value = fields.get(candidate)
        if value is not None:
            return value
    return None


def extract_content_with_cu(document_bytes: bytes, document_name: str) -> Dict[str, object]:
    """Extract content from PDF using the Azure AI Content Understanding SDK."""
    endpoint = os.environ.get("CONTENT_UNDERSTANDING_ENDPOINT")
    analyzer_id = os.environ.get("CONTENT_UNDERSTANDING_ANALYZER", "prebuilt-documentFields")

    if not endpoint:
        raise ValueError("CONTENT_UNDERSTANDING_ENDPOINT must be configured.")

    client = ContentUnderstandingClient(
        endpoint=endpoint,
        credential=get_default_credential()
    )
   
    client.update_defaults({
        "modelDeployments": {
            "gpt-4.1": "gpt-4.1-225824",
            "gpt-4.1-mini": "gpt-4.1-mini-011451",
            "text-embedding-3-large": "text-embedding-3-large-220211"
        }
    })


    page_documents = split_pdf_into_pages(document_bytes)

    field_candidate_list = get_field_candidates()
    target_field_key = field_candidate_list[0]
    content_items: List[Dict[str, object]] = []

    def analyze_page(page_pdf: bytes, page_number: int) -> Dict[str, object]:
        poller = client.begin_analyze_binary(
            analyzer_id=analyzer_id,
            binary_input=page_pdf,
            content_type="application/pdf",
        )

        result = poller.result()

        content_entry: Dict[str, object] = {
            "page_number": page_number,
            target_field_key: None,
            "fields": {},
            "operation_id": getattr(poller, "operation_id", None),
        }

        for item in getattr(result, "contents", []) or []:
            if isinstance(item, DocumentContent):
                all_fields = extract_all_field_values(item)
                content_entry["fields"] = all_fields
                content_entry[target_field_key] = resolve_field_value(all_fields, field_candidate_list)
                break

        return content_entry

    max_workers = int(os.environ.get("CONTENT_UNDERSTANDING_MAX_CONCURRENCY", "4"))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(analyze_page, page_pdf, page_number): page_number
            for page_number, page_pdf in enumerate(page_documents, start=1)
        }
        for future in as_completed(future_map):
            content_items.append(future.result())

    content_items.sort(key=lambda item: item.get("page_number", 0))

    return {
        "document_name": document_name,
        "analyzer_id": analyzer_id,
        "total_pages": len(page_documents),
        "content_items": content_items,
    }


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.route(route="extract_pdf_content")
def extract_pdf_content(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function to extract content from a PDF document.
    
    Query parameters:
        - document: Name of the PDF document in Azure Blob Storage
    
    Returns:
        JSON response with extracted content from the PDF
    """
    logging.info('PDF content extraction function triggered.')

    # Get document name from query string
    document_name = req.params.get('document')
    
    if not document_name:
        return func.HttpResponse(
            json.dumps({"error": "Please provide a 'document' parameter in the query string."}),
            status_code=400,
            mimetype="application/json"
        )
    
    # Validate document name ends with .pdf
    if not document_name.lower().endswith('.pdf'):
        return func.HttpResponse(
            json.dumps({"error": "Document must be a PDF file."}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        # Get PDF from Azure Blob Storage
        logging.info(f'Downloading document: {document_name}')
        document_bytes = get_blob_content(document_name)
        logging.info(f'Successfully downloaded document: {document_name}, size: {len(document_bytes)} bytes')
        
        # Extract content using Azure Content Understanding
        logging.info(f'Extracting content from document: {document_name}')
        extracted_content = extract_content_with_cu(document_bytes, document_name)
        logging.info(f'Successfully extracted content from document: {document_name}')
        
        return func.HttpResponse(
            json.dumps(extracted_content, indent=2, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f'Error processing document {document_name}: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Failed to process document: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )