import apache_beam as beam 
from datetime import datetime

def default_score(element):
    cols=element.split(",")
    paid_amount=float(cols[-2])
    total_amount=float(cols[-4])
    max_limit = float(cols[-5])
    if (paid_amount<0.7*total_amount) & (total_amount==max_limit):
        point=3
    elif paid_amount<0.7*total_amount:
        point=1
    elif  (total_amount==max_limit) & (paid_amount<max_limit):
        point=1
    else:
        point=0

    return (cols[0],point)

def calculate_late_payment(elements):               # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018,2000,30-01-2018]
  
  due_date = datetime.strptime(elements[6].rstrip().lstrip(), '%d-%m-%Y')           # due_date = 26-01-2018
  payment_date = datetime.strptime(elements[8].rstrip().lstrip(), '%d-%m-%Y')       # payment_date = 30-01-2018
  
  if payment_date <= due_date:
    elements.append('0') 
  else:
    elements.append('1')                           # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018,2000,30-01-2018,1]
    
  return elements

def format_output(sum_pair):
  key_name, miss_months = sum_pair
  return str(key_name) + ', ' + str(miss_months) + ' missed'

def calculate_month(input_list):        #input  [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018]
                                       
  # Convert payment_date to datetime and extract month of payment
  payment_date = datetime.strptime(input_list[8].rstrip().lstrip(), '%d-%m-%Y')  # payment_date = 30-01-2018
  input_list.append(str(payment_date.month))                                     # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018, 01]
  
  return input_list 

def calculate_personal_loan_defaulter(input):       #input key -> CT68554,Ronald Chiki   value --> [01,05,06,07,08,09,10,11,12]
    max_allowed_missed_months = 4
    max_allowed_consecutive_missing = 2
    
    name, months_list = input                                   # [CT68554,Ronald,Chiki,Serviceman,LN_8460,Personal Loan,25-01-2018,50000,25-01-2018]
      
    months_list.sort()
    sorted_months = months_list                                 # sorted_months = [01,05,06,07,08,09,10,11,12]
    total_payments = len(sorted_months)                         # total_payments = 10
    
    missed_payments = 12 - total_payments                       # missed_payments = 2

    if missed_payments > max_allowed_missed_months:             # false
       return name, missed_payments                             #  N/A
    
    consecutive_missed_months = 0

    temp = sorted_months[0] - 1                                 # temp = 0
    if temp > consecutive_missed_months:                        # false
        consecutive_missed_months = temp                        #NA

    temp = 12 - sorted_months[total_payments-1]                  
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp                        # temp = 0

    for i in range(1, len(sorted_months)):                      # [01,05,06,07,08,09,10,11,12]
        temp = sorted_months[i] - sorted_months[i-1] -1         # temp = 5-1-1 = 3
        if temp > consecutive_missed_months:
            consecutive_missed_months = temp                    # consecutive_missed_months = 3
    
    if consecutive_missed_months > max_allowed_consecutive_missing:
       return name, consecutive_missed_months                   # CT68554,Ronald Chiki   3
    
    return name, 0 

def return_tuple(element):
  thisTuple=element.split(',')
  return (thisTuple[0],thisTuple[1:])



p1 = beam.Pipeline()

card_defaulter =(

    p1
    |beam.io.ReadFromText('cards.txt',skip_header_lines=1)
    |beam.Map(default_score)
    |beam.CombinePerKey(sum)
    |beam.Filter(lambda x:x[1]>0)
    #|beam.io.WriteToText('./output/card_skip')
)

medical_loan_defaulter = (
                            p1
                            | 'Read_medical' >> beam.io.ReadFromText('loan.txt',skip_header_lines=1)
                            | 'Split Row' >> beam.Map(lambda row : row.split(','))
                            | 'Filter medical loan' >> beam.Filter(lambda element : (element[5]).rstrip().lstrip() == 'Medical Loan')
                            | 'Calculate late payment' >> beam.Map(calculate_late_payment)
                            | 'Make key value pairs' >> beam.Map(lambda elements: (elements[0] + ', ' + elements[1]+' '+elements[2], int(elements[9])) ) 
                            | 'Group medical loan based on month' >> beam.CombinePerKey(sum)
                            | 'Check for medical loan defaulter' >> beam.Filter(lambda element: element[1] >= 3)
                            | 'Format medical loan output' >> beam.Map(format_output)
                         )     

personal_loan_defaulter = (
                            p1
                            | 'Read' >> beam.io.ReadFromText('loan.txt',skip_header_lines=1)   
                            | 'Split' >> beam.Map(lambda row : row.split(','))
                            | 'Filter personal loan' >> beam.Filter(lambda element : (element[5]).rstrip().lstrip() == 'Personal Loan')
                            | 'Split and Append New Month Column' >> beam.Map(calculate_month)   
                            | 'Make key value pairs loan' >> beam.Map(lambda elements: (elements[0] + ', ' + elements[1]+' '+elements[2], int(elements[9])) ) 
                            | 'Group personal loan based on month' >> beam.GroupByKey()
                            | 'Check for personal loan defaulter' >> beam.Map(calculate_personal_loan_defaulter)
                            | 'Filter only personal loan defaulters' >> beam.Filter(lambda element: element[1] > 0)
                            | 'Format personal loan output' >> beam.Map(format_output)
                          )  


final_loan_defaulters = (
                          ( personal_loan_defaulter, medical_loan_defaulter )
                          | 'Combine all defaulters' >> beam.Flatten()
                          #| 'Write all defaulters to text file' >> beam.io.WriteToText('outputs/loan_defaulters')
                          | 'tuple for loan' >> beam.Map(return_tuple)
                        )  
                        
both_defaulters =  (
                    {'card_defaulter': card_defaulter, 'loan_defaulter': final_loan_defaulters}
                    | beam.CoGroupByKey()
                    |'Write p3 results' >> beam.io.WriteToText('outputs/both.txt')
                   )            

p1.run()