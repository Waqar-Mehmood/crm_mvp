from django.shortcuts import render
from .models import Company


def company_list(request):
    # include related phone/email/social records for efficiency
    companies = (
        Company.objects
        .prefetch_related("phones", "emails", "social_links")
        .order_by("name")
    )
    return render(request, "crm/company_list.html", {"companies": companies})