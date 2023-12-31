# Generated by Django 3.1.14 on 2023-08-23 17:06

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='BusinessHours',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('store_id', models.TextField()),
                ('day', models.PositiveIntegerField(max_length=7)),
                ('start_time_local', models.TextField(max_length=40)),
                ('end_time_local', models.TextField(max_length=40)),
            ],
        ),
        migrations.CreateModel(
            name='StoreStatus',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('store_id', models.TextField()),
                ('status', models.CharField(max_length=20)),
                ('timestamp_utc', models.TextField(max_length=40)),
            ],
        ),
        migrations.CreateModel(
            name='Timezone',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('store_id', models.TextField()),
                ('timezone_str', models.TextField()),
            ],
        ),
    ]
