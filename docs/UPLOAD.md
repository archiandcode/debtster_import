# /upload endpoint (Go importer)

This endpoint is intended to replace the Laravel `importFile` route for file uploads. It stores the uploaded file to the configured S3 (MinIO) bucket and creates an `import_record` in MongoDB.

Endpoint:
  - multipart/form-data
  - fields: `file` (file), `action` or `type` (string)

Response (201):
{
  "id": "<mongo-id or string id>",
  "path": "s3://<bucket>/<key>"
}

Notes for frontend:

Authentication / Laravel Sanctum
- The `/upload` endpoint is protected with a Sanctum-compatible token check.
- Provide a valid Laravel personal access token in either the `Authorization: Bearer <token>` header or via the `?token=<token>` query parameter.
- When the request is authenticated the service will store the uploader's user id in the created `import_record.user_id` field. This ID is the same `id` used in the Laravel DB for the User model.

Example (authenticated):
```bash
curl -H "Authorization: Bearer <your_laravel_pat>" -F "file=@./data/import.csv" -F "action=import_debtors" http://localhost:8070/upload
```
Example:
```bash
curl -F "file=@./data/import.csv" -F "action=import_debtors" http://localhost:8070/upload
```
