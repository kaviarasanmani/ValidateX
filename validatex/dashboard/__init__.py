"""
ValidateX Multi-Dataset Web Dashboard (`validatex.dashboard`).
"""

from validatex.dashboard.server import (
    build_dashboard_payload,
    create_wsgi_app,
    export_dashboard_html,
    render_dashboard_html,
    serve_dashboard,
)

__all__ = [
    "build_dashboard_payload",
    "create_wsgi_app",
    "export_dashboard_html",
    "render_dashboard_html",
    "serve_dashboard",
]
