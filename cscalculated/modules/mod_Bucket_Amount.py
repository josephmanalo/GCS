from models import *
from sqlalchemy.sql import func
from collections import defaultdict

def Bucket_Amount(session):
    """
    Refer to invoice table PCC_Invoices
    If there is only 1 open invoice (INVOICE_STATUS = O) - the 'INVOICE_CLOSING_BALANCE' of that invoice is considered as Bucket1Amount. Else the Bucket1 amount is 0.

    If there are 2 open invoices, the second oldest invoice will be considered as Bucket2. Consider the 'INVOICE_CLOSING_BALANCE' of that invoice as Bucket2Amount. Else the Bucket2 amount is 0 and so on.

    For Bucket10 all subsequent invoice closing balance needs to be added up.

    This is to be Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

    """

    Invoices = session.query(PCC_Invoices.GBU_ID,PCC_Invoices.Group_ID,PCC_Invoices.Entity_ID,PCC_Invoices.Corp_ID,PCC_Invoices.Customer_ID, PCC_Invoices.invoices_closing_balance).filter(PCC_Invoices.invoice_status=='O')
    
    gbu_dict = defaultdict(list)
    group_dict = defaultdict(list)
    entity_dict = defaultdict(list)
    corp_dict = defaultdict(list)
    cust_dict = defaultdict(list)

    for i in Invoices:
        gbu_dict[i.GBU_ID].append(i.invoices_closing_balance)
        group_dict[i.Group_ID].append(i.invoices_closing_balance)
        entity_dict[i.Entity_ID].append(i.invoices_closing_balance)
        corp_dict[i.Corp_ID].append(i.invoices_closing_balance)
        cust_dict[i.Customer_ID].append(i.invoices_closing_balance)

    for group in [gbu_dict,group_dict,entity_dict,corp_dict,cust_dict]:
        for key,value in group.items():
            if(len(value) < 10):
                for _ in range(0,10-len(value)):
                    value.append(0)
            elif(len(value) > 10):
                value[9] = sum(value[9:])
                group[key] = value[:10]

    for key,value in gbu_dict.items():
        new_record = PCC_GBUAGG_DATA(GBU_ID=key)
        new_record(**{f"Bucket{x+1}_Amount":v for x,v in enumerate(value)})
        session.add(new_record)

    for key,value in group_dict.items():
        new_record = PCC_COMPGAGG_DATA(Group_ID=key)
        new_record(**{f"Bucket{x+1}_Amount":v for x,v in enumerate(value)})
        session.add(new_record)
            
    for key,value in entity_dict.items():
        new_record = PCC_ENTITYAGG_DATA(Entity_ID=key)
        new_record(**{f"Bucket{x+1}_Amount":v for x,v in enumerate(value)})
        session.add(new_record)    

    for key,value in corp_dict.items():
        new_record = PCC_CORPAGG_DATA(Corp_ID=key)
        new_record(**{f"Bucket{x+1}_Amount":v for x,v in enumerate(value)})
        session.add(new_record)    

    for key,value in cust_dict.items():
        new_record = PCC_CUSTAGG_DATA(Customer_ID=key)
        new_record(**{f"Bucket{x+1}_Amount":v for x,v in enumerate(value)})
        session.add(new_record)
