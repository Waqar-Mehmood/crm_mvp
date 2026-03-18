from django import template

from crm.channel_choices import humanize_channel_value

register = template.Library()


@register.filter
def channel_display(value):
    return humanize_channel_value(value)
