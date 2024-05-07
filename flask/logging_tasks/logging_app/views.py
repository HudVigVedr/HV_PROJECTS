from django.shortcuts import render

# Create your views here.
from logging_app.models import MyModel

entries = MyModel.objects.all()
for entry in entries:
    print(entry.name, entry.description)