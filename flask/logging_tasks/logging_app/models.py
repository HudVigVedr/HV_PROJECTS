from django.db import models



class MyModel(models.Model):
    name = models.CharField(max_length=100)
    description = models.TextField()
    
    # Add more fields as needed
    
    def __str__(self):
        return self.name