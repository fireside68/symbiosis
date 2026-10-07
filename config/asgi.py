"""
Asynchronous Server Gateway Interface (ASGI) config for config project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/asgi/
"""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django_asgi_app = get_asgi_application()  # must run before importing anything that touches models

from channels.auth import AuthMiddlewareStack  # noqa: E402
from channels.routing import ProtocolTypeRouter, URLRouter  # noqa: E402
from django.urls import path  # noqa: E402
from strawberry.channels import GraphQLWSConsumer  # noqa: E402

from api.schema import schema  # noqa: E402

application = ProtocolTypeRouter(
    {
        "http": django_asgi_app,
        # AuthMiddlewareStack = CookieMiddleware + SessionMiddleware + AuthMiddleware,
        # so scope["user"] is populated from the session cookie the same way
        # request.user is for the HTTP GraphQLView below.
        "websocket": AuthMiddlewareStack(
            URLRouter(
                [path("graphql/", GraphQLWSConsumer.as_asgi(schema=schema))]
            )
        ),
    }
)

