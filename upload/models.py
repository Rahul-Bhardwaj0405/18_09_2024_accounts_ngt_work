# models.py
from django.db import models

class BookingData(models.Model):
    BANK_CHOICES = [
        ('hdfc', 'HDFC'),
        ('icici', 'ICICI'),
        ('karur_vysya', 'Karur Vysya Bank'),
    ]

    bank_name = models.CharField(max_length=50, choices=BANK_CHOICES)
    year = models.IntegerField()
    month = models.CharField(max_length=20)
    sale_total = models.IntegerField()  # Store the total number of entries
    date = models.DateField()  # Store the date from "CREDITED ON"
    sale_amount = models.DecimalField(max_digits=10, decimal_places=2)  # Store the "BOOKING AMOUNT"

    def __str__(self):
        return f"{self.bank_name} - {self.year}-{self.month} - Booking"


class RefundData(models.Model):
    pass
    # BANK_CHOICES = [
    #     ('hdfc', 'HDFC'),
    #     ('icici', 'ICICI'),
    #     ('karur_vysya', 'Karur Vysya Bank'),
    # ]

    # bank_name = models.CharField(max_length=50, choices=BANK_CHOICES)
    # year = models.IntegerField()
    # month = models.CharField(max_length=20)
    # sale_total = models.IntegerField()  # Store the total number of entries
    # date = models.DateField()  # Store the date from "CREDITED ON"
    # sale_amount = models.DecimalField(max_digits=10, decimal_places=2)  # Store the "BOOKING AMOUNT"

    # def __str__(self):
    #     return f"{self.bank_name} - {self.year}-{self.month} - Refund"
