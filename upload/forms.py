# forms.py
from django import forms

class UploadForm(forms.Form):
    bank_name = forms.ChoiceField(choices=[('hdfc', 'HDFC'), ('icici', 'ICICI'), ('karur_vysya', 'Karur Vysya Bank')])  # Add Karur Vysya Bank
    year = forms.CharField(max_length=4)
    month = forms.CharField(max_length=2)
    booking_or_refund = forms.ChoiceField(choices=[('booking', 'Booking'), ('refund', 'Refund')])
    
    # Add merchant name field with options
    merchant_name = forms.ChoiceField(choices=[
        ('irctc_web', 'IRCTC Web'),
        ('irctc_app', 'IRCTC App'),
        ('irctc_air_ticket', 'IRCTC Air Ticket'),
        ('irctc_tourism', 'IRCTC Tourism'),
        ('all', 'All')
    ])
