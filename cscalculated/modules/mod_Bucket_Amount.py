from models import *
from sqlalchemy.sql import func
from collections import defaultdict

def groupings(**kwargs):
    for i in kwargs:
        print(f"{i}{kwargs}")

def Bucket_Amount(session):
    """
    Refer to invoice table PCC_Invoices
    If there is only 1 open invoice (INVOICE_STATUS = O) - the 'INVOICE_CLOSING_BALANCE' of that invoice is considered as Bucket1Amount. Else the Bucket1 amount is 0.

    If there are 2 open invoices, the second oldest invoice will be considered as Bucket2. Consider the 'INVOICE_CLOSING_BALANCE' of that invoice as Bucket2Amount. Else the Bucket2 amount is 0 and so on.

    For Bucket10 all subsequent invoice closing balance needs to be added up.

    This is to be Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

    """

    Invoices = session.query(PCC_Invoices.account_id, PCC_Invoices.invoices_closing_balance).filter(PCC_Invoices.invoice_status=='O'\
                            ).group_by(PCC_Invoices.account_id,PCC_Invoices.invoices_closing_balance).order_by(PCC_Invoices.account_id.desc()).all()
    
    invoice_dict = defaultdict(lambda: defaultdict(int))
    prev = 0
    for invoices in Invoices:
        if prev != invoices[0]:
            prev = invoices[0]
            ctr=0
            
        ctr += 1
        if ctr >= 10:
            invoice_dict[invoices[0]][f"Bucket10_Amount"] += invoices[1]
        else:
            invoice_dict[invoices[0]][f"Bucket{ctr}_Amount"] += invoices[1]

    mapping = {i: groupings for i in invoice_dict}
        
    for group,values in invoice_dict.items():
        print(mapping[group](**values))
        # session.add(mapping[group](**values))

    # session.commit()      
    