#!/usr/bin/env python3
"""
Document extraction utilities for handling various archive formats.

Supports extracting documents from:
- .edoc files (ASIC-E ZIP containers used in Latvian e-document system)
- .zip files
- .rar files (if unrar is available)
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class ExtractedDocument:
    """Represents a document extracted from an archive."""

    def __init__(self, filename: str, content: bytes, source_archive: str):
        self.filename = filename
        self.content = content
        self.source_archive = source_archive
        self.extension = Path(filename).suffix.lower()

    def is_document(self) -> bool:
        """Check if this is a readable document format."""
        return self.extension in {'.pdf', '.doc', '.docx', '.odt', '.txt', '.rtf'}

    def __repr__(self):
        return f"ExtractedDocument(filename='{self.filename}', size={len(self.content)}, ext='{self.extension}')"


def extract_from_edoc(edoc_path: str | Path) -> List[ExtractedDocument]:
    """
    Extract documents from .edoc archive (ASIC-E container).

    .edoc files are ZIP archives containing:
    - Main document (PDF, DOC, DOCX, etc.)
    - META-INF/ folder with signatures and manifest
    - mimetype file

    Returns:
        List of ExtractedDocument objects for readable documents
    """
    edoc_path = Path(edoc_path)
    documents = []

    try:
        with zipfile.ZipFile(edoc_path, 'r') as zf:
            for file_info in zf.filelist:
                filename = file_info.filename

                # Skip metadata and system files
                if any(skip in filename.lower() for skip in [
                    'meta-inf/', 'mimetype', '__macosx/', '.ds_store',
                    'signatures', 'manifest.xml', '.txt'  # Skip signature metadata
                ]):
                    continue

                # Skip directories
                if filename.endswith('/'):
                    continue

                # Extract file
                try:
                    content = zf.read(filename)
                    doc = ExtractedDocument(
                        filename=filename,
                        content=content,
                        source_archive=str(edoc_path)
                    )

                    # Only keep document files
                    if doc.is_document():
                        documents.append(doc)
                        logger.info(f"Extracted {filename} from {edoc_path.name} ({len(content)} bytes)")

                except Exception as e:
                    logger.warning(f"Failed to extract {filename} from {edoc_path.name}: {e}")
                    continue

    except zipfile.BadZipFile:
        logger.error(f"{edoc_path.name} is not a valid ZIP/EDOC file")
    except Exception as e:
        logger.error(f"Failed to process {edoc_path.name}: {e}")

    return documents


def extract_from_zip(zip_path: str | Path) -> List[ExtractedDocument]:
    """
    Extract documents from regular .zip archive.

    Returns:
        List of ExtractedDocument objects for readable documents
    """
    # .edoc files are also ZIP files, so we can reuse the same logic
    return extract_from_edoc(zip_path)


def extract_from_rar(rar_path: str | Path) -> List[ExtractedDocument]:
    """
    Extract documents from .rar archive.

    Requires 'unrar' or 'rarfile' to be available.

    Returns:
        List of ExtractedDocument objects for readable documents
    """
    rar_path = Path(rar_path)
    documents = []

    try:
        import rarfile

        with rarfile.RarFile(rar_path, 'r') as rf:
            for file_info in rf.infolist():
                filename = file_info.filename

                # Skip metadata and system files
                if any(skip in filename.lower() for skip in [
                    '__macosx/', '.ds_store'
                ]):
                    continue

                # Skip directories
                if file_info.isdir():
                    continue

                # Extract file
                try:
                    content = rf.read(filename)
                    doc = ExtractedDocument(
                        filename=filename,
                        content=content,
                        source_archive=str(rar_path)
                    )

                    # Only keep document files
                    if doc.is_document():
                        documents.append(doc)
                        logger.info(f"Extracted {filename} from {rar_path.name} ({len(content)} bytes)")

                except Exception as e:
                    logger.warning(f"Failed to extract {filename} from {rar_path.name}: {e}")
                    continue

    except ImportError:
        logger.error("rarfile module not available. Install with: pip install rarfile")
    except Exception as e:
        logger.error(f"Failed to process {rar_path.name}: {e}")

    return documents


def extract_documents_from_archive(archive_path: str | Path) -> List[ExtractedDocument]:
    """
    Auto-detect archive type and extract documents.

    Supports: .edoc, .zip, .rar

    Args:
        archive_path: Path to archive file

    Returns:
        List of ExtractedDocument objects
    """
    archive_path = Path(archive_path)
    extension = archive_path.suffix.lower()

    if extension in {'.edoc', '.asice', '.zip'}:
        return extract_from_edoc(archive_path)
    elif extension == '.rar':
        return extract_from_rar(archive_path)
    else:
        logger.warning(f"Unsupported archive format: {extension}")
        return []


# Convenience function for text extraction integration
def get_archive_content_as_bytes(archive_path: str | Path) -> Optional[bytes]:
    """
    Extract the main document from an archive and return its bytes.

    Useful for direct integration with text extraction functions.
    Returns the first document found, or None if no documents.
    """
    documents = extract_documents_from_archive(archive_path)

    if not documents:
        return None

    # Return the largest document (likely the main one)
    main_doc = max(documents, key=lambda d: len(d.content))
    return main_doc.content
