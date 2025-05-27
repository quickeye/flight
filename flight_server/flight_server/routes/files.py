from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from pathlib import Path
import duckdb
import tzlocal
from datetime import datetime
import logging

from ..file_discovery import get_file_discovery_service


router = APIRouter(
    prefix="/files",
    tags=["files"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

@router.get("/registry", summary="List registered files")
async def list_registered_files(
    path: Optional[str] = Query(None, description="Filter by path (supports wildcard)"),
    file_type: Optional[str] = Query(None, description="Filter by file extension"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    service = Depends(get_file_discovery_service)
) -> Dict[str, Any]:
    """
    List all registered files with optional filtering.
    Results are paginated and can be filtered by path and file type.
    """
    try:
        # Get files from the MinIO discovery service
        files = service.get_file_list(file_type=file_type)
        
        # Apply path filter if provided
        if path:
            import fnmatch
            files = [f for f in files if fnmatch.fnmatch(f['path'], path)]
        
        # Apply pagination
        total = len(files)
        paginated_files = files[offset:offset + limit]
        
        return {
            "files": paginated_files,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list files")

@router.post("/discovery/scan", status_code=202, summary="Trigger a file discovery scan")
async def trigger_scan(
    service = Depends(get_file_discovery_service)
) -> Dict[str, str]:
    """
    Trigger an immediate scan of the MinIO bucket to discover and register files.
    This operation runs asynchronously in the background.
    """
    try:
        # Run the scan in a separate thread
        import threading
        def run_scan():
            try:
                service.scan_and_update()
            except Exception as e:
                logger.error(f"Error during scan: {str(e)}", exc_info=True)
        
        thread = threading.Thread(target=run_scan, daemon=True)
        thread.start()
        
        return {
            "status": "scan_started",
            "message": "Background scan started successfully"
        }
    except Exception as e:
        logger.error(f"Error triggering scan: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to start scan")

@router.get("/registry", summary="List registered files")
async def list_registered_files(
    path: Optional[str] = Query(None, description="Filter by path (supports wildcard)"),
    file_type: Optional[str] = Query(None, description="Filter by file extension"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    service = Depends(get_file_discovery_service)
) -> Dict[str, Any]:
    """
    List all registered files with optional filtering.
    Results are paginated and can be filtered by path and file type.
    """
    try:
        conn = service.get_connection()
        query = "SELECT * FROM file_registry WHERE 1=1"
        params = []
        
        if path:
            query += " AND path LIKE ?"
            params.append(f"%{path}%")
        if file_type:
            query += " AND file_type = ?"
            params.append(file_type)
            
        # Get total count for pagination
        count_query = f"SELECT COUNT(*) FROM ({query})"
        total = conn.execute(count_query, params).fetchone()[0]
        
        # Add ordering and pagination
        query += " ORDER BY last_modified DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        # Execute query
        result = conn.execute(query, params).fetchall()
        
        # Format response
        files = [
            {
                "id": row[0],
                "path": row[1],
                "size_bytes": row[2],
                "last_modified": row[3].isoformat() if row[3] else None,
                "registered_at": row[4].isoformat() if row[4] else None,
                "file_type": row[5]
            }
            for row in result
        ]
        
        return {
            "files": files,
            "pagination": {
                "total": total,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + len(files)) < total
            }
        }
        
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list files")

@router.get("/registry/count", summary="Get file count statistics")
async def get_file_counts(
    service = Depends(get_file_discovery_service)
) -> Dict[str, Any]:
    """
    Get counts of files by type and total count.
    Returns a dictionary with total count and counts by file type.
    """
    try:
        # Get all files
        files = service.get_file_list()
        
        # Initialize counts
        counts = {
            "total": len(files),
            "by_type": {}
        }
        
        # Count files by type
        for file in files:
            file_type = file.get('file_type', 'unknown')
            if file_type not in counts["by_type"]:
                counts["by_type"][file_type] = 0
            counts["by_type"][file_type] += 1
            
        return counts
        
    except Exception as e:
        logger.error(f"Error getting file counts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get file counts")

@router.get("/registry/types", summary="Get list of file types")
async def get_file_types(
    service = Depends(get_file_discovery_service)
) -> List[str]:
    """
    Get a list of all unique file types in the registry.
    """
    try:
        # Get all files
        files = service.get_file_list()
        
        # Extract unique file types
        file_types = set()
        for file in files:
            if 'file_type' in file and file['file_type']:
                file_types.add(file['file_type'])
        
        return sorted(list(file_types))
    except Exception as e:
        logger.error(f"Error getting file types: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get file types")

@router.get("/discovery/status", summary="Get file discovery service status")
async def get_discovery_status(
    service = Depends(get_file_discovery_service)
) -> Dict[str, Any]:
    """
    Get the current status of the file discovery service.
    Returns information about the service configuration and current state.
    """
    try:
        local_tz = tzlocal.get_localzone()
        last_updated = None
        last_updated_human = "Never"
        
        if hasattr(service, 'last_updated') and service.last_updated:
            # Ensure the datetime is timezone-aware
            if service.last_updated.tzinfo is None:
                # If not timezone-aware, assume UTC
                last_updated_dt = service.last_updated.replace(tzinfo=timezone.utc)
            else:
                last_updated_dt = service.last_updated
                
            last_updated = last_updated_dt.isoformat()
            # Convert to local timezone for human-readable format
            last_updated_local = last_updated_dt.astimezone(local_tz)
            last_updated_human = last_updated_local.strftime("%Y-%m-%d %H:%M:%S %Z")

        status = {
            "status": "running" if hasattr(service, '_thread') and service._thread and service._thread.is_alive() else "stopped",
            "bucket": service.bucket_name,
            "endpoint": service.endpoint_url,
            "scan_interval_seconds": service.scan_interval,
            "database": service.db_path,
            "last_updated": last_updated,
            "last_updated_human": last_updated_human,
            "timezone": str(local_tz)  # Add timezone info to the response
        }
        return status
    except Exception as e:
        logger.error(f"Error getting discovery status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get discovery status")