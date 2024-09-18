from celery import shared_task
import pandas as pd
import json
import logging
from io import StringIO, BytesIO
from .models import BookingData, RefundData  # Import models
import base64
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@shared_task
def process_uploaded_files(file_content_base64, file_name, bank_name, year, month, booking_or_refund):
    file_content = base64.b64decode(file_content_base64)
    print(file_content)
    print(file_name)
    try:
        # Initialize an empty DataFrame
        df = pd.DataFrame()

        # Read the file content into a DataFrame based on file type
        try:
            if file_name.endswith('.csv'):
                df = pd.read_csv(StringIO(file_content.decode('utf-8')))
            elif file_name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(BytesIO(file_content), engine='openpyxl')
            elif file_name.endswith('.txt'):
                df = pd.read_csv(StringIO(file_content.decode('utf-8')), delimiter='\t')
            elif file_name.endswith('.json'):
                data = json.loads(file_content)
                df = pd.json_normalize(data)
            else:
                file_str = file_content.decode('utf-8')
                delimiter = ',' if ',' in file_str else '\t'
                df = pd.read_csv(StringIO(file_str), delimiter=delimiter)
            logging.info(f"File read successfully: {file_name}")
        except Exception as e:
            logging.error(f"Error reading file {file_name}: {e}")
            return

        # Clean the column names to remove unwanted spaces and periods
        df.columns = df.columns.str.strip().str.replace('.', '', regex=False)
        logging.info(f"Column names cleaned: {df.columns.tolist()}")

        # Extract required columns based on bank and booking/refund
        try:
            if bank_name == 'Karur Vysya Bank':
                # Extract sale total (count of rows)
                sale_total = df['SNO'].count()  # Assumes 'S.NO.' cleaned to 'SNO'
                logging.info(f"Sale total (count of rows): {sale_total}")

                # Extract date from 'CREDITED ON'
                credited_on_dates = pd.to_datetime(df['CREDITED ON'])
                date = credited_on_dates.iloc[0].date() if not credited_on_dates.empty else None
                logging.info(f"Date extracted: {date}")

                # Extract total sale amount (sum of 'BOOKING AMOUNT')
                sale_amount = float(pd.to_numeric(df['BOOKING AMOUNT'], errors='coerce').fillna(0).sum())
                logging.info(f"Total sale amount: {sale_amount}")

                # Save to the database
                if booking_or_refund == 'booking':
                    BookingData.objects.create(
                        bank_name=bank_name,
                        year=year,
                        month=month,
                        sale_total=sale_total,
                        date=date,
                        sale_amount=sale_amount
                    )
                    logging.info(f"Booking data saved successfully for file: {file_name}.")
                elif booking_or_refund == 'refund':
                    RefundData.objects.create(
                        bank_name=bank_name,
                        year=year,
                        month=month,
                        sale_total=sale_total,
                        date=date,
                        sale_amount=sale_amount
                    )
                    logging.info(f"Refund data saved successfully for file: {file_name}.")
                else:
                    logging.error(f"Unknown booking_or_refund type: {booking_or_refund}")

            else:
                logging.error(f"Unknown bank type: {bank_name}")

        except Exception as e:
            logging.error(f"Error extracting data from file {file_name}: {e}")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
