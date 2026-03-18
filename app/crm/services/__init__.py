"""Service-layer helpers for the CRM app.

Prefer importing reusable business helpers from this package instead of the
legacy root-level utility modules in ``crm/`` when touching new code. Feature-
specific services for companies and contacts can live in sibling modules here
without affecting the existing import/export flow.
"""
