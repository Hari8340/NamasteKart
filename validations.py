import os
import datetime as dt
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
class namasteKart:
    def GetIncomingData(self):
        try:
            today_date = str(dt.date.today()).split('-')
            joined_date = ''.join(today_date)
            base_folder = 'C:\\NamasteKart\\Incoming_files'
            folder_path = os.path.join(base_folder, joined_date)
            file_list = os.listdir(folder_path)
            data_list = []
            file_count=0
            success_count=0
            error_count=0
            for file in file_list:
                file_count+=1
                file_path = os.path.join(folder_path, file)
                df = pd.read_csv(file_path)
                with open(file_path, mode='r') as f:
                    count = 0
                    lines = f.readlines()
                    data_list.extend(tuple(line.strip().split(',')) for line in lines)

                    my_incoming_dict = {order_id: {'order_id': order_id, 'order_date': order_date,
                                                'product_id': product_id, 'quantity': quantity, 'sales': sales, 'city': city}
                                        for order_id, order_date, product_id, quantity, sales, city in data_list if order_id.isdigit()}
                    namaste=namasteKart()
                    master_list_data = namaste.GetDataFromMaster()
                    print(my_incoming_dict)
                    for key, value in my_incoming_dict.items():

                        master_data = master_list_data.get(value['product_id'], {})
                        mas_product_id = master_data.get('product_id', '')
                        mas_price = master_data.get('price', '')
                        child_order_date = value.get('order_date', '')
                        child_order_id = value.get('order_id', '')
                        child_order_quantity = value.get('quantity', '')
                        child_sales = value.get('sales', '')

                        print(f"ordered date {file}", child_order_date)
                        print(str(dt.date.today()))
                        
                        try:
                            if child_order_date > str(dt.date.today()):
                                print('inside if condition')
                                df['message'] = ''
                                df.loc[df['order_id'] == int(child_order_id), 'message'] = 'Date limit is exceeded'
                                count += 1
                        except ValueError as ve:
                            print(f"Error updating 'Date limit is exceeded': {ve}")

                        try:
                            if int(child_order_quantity) * int(mas_price) != int(child_sales):
                                df.loc[df['order_id'] == int(child_order_id), 'message'] = 'price is not matching'
                                print(df)
                                count += 1
                        except ValueError as ve:
                            print(f"Error updating 'price is not matching': {ve}")

                    if count == 0:
                        success_count+=1
                        print("inside if dataframe save block ")
                        out_put_folder = f'C:\\NamasteKart\\success_files\\{joined_date}'
                        if not os.path.exists(out_put_folder):
                            os.makedirs(out_put_folder)
                        output_file_path = os.path.join(out_put_folder, file)
                        df.to_csv(output_file_path, index=False)
                    else:
                        print("inside else dataframe save block ")
                        out_put_folder = f'C:\\NamasteKart\\error_files\\{joined_date}'
                        if not os.path.exists(out_put_folder):
                            os.makedirs(out_put_folder)
                        output_file_path = os.path.join(out_put_folder, file)
                        df.to_csv(output_file_path, index=False)
            email_body = f"Total {file_count} incoming files,{success_count} successful files and {file_count-success_count} rejected files for the day"
            namaste.send_email("Test Mail",email_body,"harishnakka49@gmail.com")              

        except Exception as e:
            print(f"An error occurred: {e}")

    def GetDataFromMaster(self):
        master_folder = f'C:\\NamasteKart\\product_master.csv'
        master_data = []
        master_data_dict = {}
        try:
            with open(master_folder, mode='r') as f:
                lines = f.readlines()

            master_data.extend(tuple(line.strip().split(',')) for line in lines)
            master_data_dict = {product_id: {'product_id': product_id, 'product_name': product_name, 'price': price, 'category': category}
                                for product_id, product_name, price, category in master_data if product_id.isdigit()}

        except Exception as e:
            print(f"An error occurred in GetDataFromMaster: {e}")

        return master_data_dict
    def send_email(self,subject, body, to_email):
    
        sender_email = "harishnakka49@gmail.com"  # Your email address
        sender_password = "wirg zyhe gzip mbio"  # we need to register app in gmail multifactor authentication and use app specific password as email password
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = to_email
        message["Subject"] = subject
        message.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, to_email, message.as_string())
            print("Email sent successfully.")
        except Exception as e:
            print(f"Error sending email: {e}")

        


