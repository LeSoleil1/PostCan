from typing import Text
from django.db import models
from django.contrib.auth.models import User
from django.db.models.deletion import CASCADE
from django.db.models.fields.related import create_many_to_many_intermediary_model


JSON_CHOICES = (
    ('Unchecked'   , 'unchecked'),
    ('Checked'  , 'checked'),
)
# Create your models here.
class Post(models.Model):
    image       = models.ImageField(upload_to = 'images/')
    description = models.TextField()
    author      = models.ForeignKey(User,on_delete=models.CASCADE)
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)

class Company(models.Model):
    name        = models.CharField(max_length=100)

    def __str__(self) -> str:
        return "ID: {}, Name: {}".format(str(self.id),self.name)

class ML_model(models.Model):
    model_file  = models.FileField(upload_to='ML_models/')
    description = models.TextField()
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return "%s" %(self.description)

class VideoPost(models.Model):
    user        = models.ForeignKey(User, on_delete=models.CASCADE)
    company_id  = models.ForeignKey(Company,on_delete=models.CASCADE)
    title       = models.CharField(max_length=100)
    description = models.TextField()
    video_file  = models.FileField(upload_to='videos/')
    thumbnail   = models.ImageField(upload_to='videos/thumbnail/', default='none')
    category    = models.CharField(max_length=50, default='none')
    created     = models.DateField(auto_now_add=True)


    def __str__(self):
        return self.title

class JsonFile(models.Model):
    json_file   = models.FileField(upload_to='jsons/')
    created     = models.DateField(auto_now_add=True)
    condition   = models.CharField(choices=JSON_CHOICES, default= 'unchecked', max_length=30)
    video_id    = models.ForeignKey(VideoPost,on_delete=CASCADE)
    ML_model_id = models.ForeignKey(ML_model,on_delete=CASCADE)

class Comment(models.Model):
    post        = models.ForeignKey(Post,on_delete=models.CASCADE)
    text        = models.TextField()
    author      = models.ForeignKey(User,on_delete=models.CASCADE)
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)

class VideoComment(models.Model):
    post        = models.ForeignKey(VideoPost,on_delete=models.CASCADE)
    text        = models.TextField()
    author      = models.ForeignKey(User,on_delete=models.CASCADE)
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)


class ModelComment(models.Model):
    post        = models.ForeignKey(ML_model,on_delete=models.CASCADE)
    text        = models.TextField()
    author      = models.ForeignKey(User,on_delete=models.CASCADE)
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)

class JSONComment(models.Model):
    post        = models.ForeignKey(JsonFile,on_delete=models.CASCADE)
    text        = models.TextField()
    author      = models.ForeignKey(User,on_delete=models.CASCADE)
    created     = models.DateTimeField(auto_now_add=True)
    modified    = models.DateTimeField(auto_now=True)

# class Documents(models.Model):
#     title       = models.CharField(max_length=50)
#     doc_files   = models.FileField(upload_to='documents/')
#     created     = models.DateField(auto_now_add=True)
#     modified    = models.DateTimeField(auto_now=True)
#     video_id    = models.ForeignKey(VideoPost,on_delete=CASCADE)
#     json_id    = models.ForeignKey(JsonFile,on_delete=CASCADE)

# class Image(models.Model):
#     image       = models.ImageField(upload_to = 'images/')
#     created     = models.DateTimeField(auto_now_add=True)
#     modified    = models.DateTimeField(auto_now=True)
#     video_id    = models.ForeignKey(VideoPost,on_delete=CASCADE)
#     json_id     = models.ForeignKey(JsonFile,on_delete=CASCADE)
#     # description = models.TextField()

# class Flaw(models.Model):
#     x_start     = models.IntegerField()
#     x_offset    = models.IntegerField()
#     y_start     = models.IntegerField()
#     y_offset    = models.IntegerField()
#     image_id    = models.ForeignKey(Image,on_delete=CASCADE)

