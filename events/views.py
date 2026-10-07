from django.shortcuts import render


def dashboard(request):
    """Static shell — everything after this is the GraphQL schema talking
    to itself over fetch() and a graphql-ws subscription. No server-rendered
    state here on purpose, so the page is exercising the same API a real
    client would."""
    return render(request, "events/dashboard.html")
