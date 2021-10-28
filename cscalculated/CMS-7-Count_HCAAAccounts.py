"""
Count of account from 'PCC_Account' where ON_HOLD_FLAG = 'Y'. 
Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
"""


from models import *
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker




db = create_engine("postgresql+psycopg2://postgres:Admin8898@localhost:5432/cmsysdev")
metadata = MetaData(db)
connection = db.connect()
Session = sessionmaker(db)
session = Session()




#PCC_Account.GBU
GBU_HCA_Acc = session.query(
    PCC_Account.GBU,
    func.count(PCC_Account.on_hold_flag).label("Count")
).filter(PCC_Account.on_hold_flag == 'Y').group_by(PCC_Account.GBU, PCC_Account.on_hold_flag)

#PCC_Account.Custom_Group
Custom_Group_HCA_Acc = session.query(
    PCC_Account.Custom_Group,
    func.count(PCC_Account.on_hold_flag).label("Count")
).filter(PCC_Account.on_hold_flag == 'Y').group_by(PCC_Account.Custom_Group, PCC_Account.on_hold_flag)

#PCC_Account.Legal_Entitiy
Legal_Entity_HCA_Acc = session.query(
    PCC_Account.Legal_Entity,
    func.count(PCC_Account.on_hold_flag).label("Count")
).filter(PCC_Account.on_hold_flag == 'Y').group_by(PCC_Account.Legal_Entity, PCC_Account.on_hold_flag)

#PCC_Account.Corp_ID
Corp_ID_HCA_Acc = session.query(
    PCC_Account.Corp_ID,
    func.count(PCC_Account.on_hold_flag).label("Count")
).filter(PCC_Account.on_hold_flag == 'Y').group_by(PCC_Account.Corp_ID, PCC_Account.on_hold_flag)

#PCC_Account.Customer_Id
Customer_ID_HCA_Acc = session.query(
    PCC_Account.Customer_ID,
    func.count(PCC_Account.on_hold_flag).label("Count")
).filter(PCC_Account.on_hold_flag == 'Y').group_by(PCC_Account.Customer_ID, PCC_Account.on_hold_flag)




# Insert into DB 
#PrimaryKey is Auto Increment

for i in GBU_HCA_Acc:
    session.add(
        PCC_GBUAGG_DATA(CNT_HCA_ACC=i.Count)
    )

for i in Custom_Group_HCA_Acc:
    session.add(
        PCC_COMPGAGG_DATA(CNT_HCA_ACC=i.Count)
    )

for i in Legal_Entity_HCA_Acc:
    session.add(
        PCC_ENTITYAGG_DATA(CNT_HCA_ACC=i.Count)
    )

for i in Corp_ID_HCA_Acc:
    session.add(
        PCC_CORPAGG_DATA(CNT_HCA_ACC=i.Count)
    )
    
for i in Customer_ID_HCA_Acc:
    session.add(
        PCC_CUSTAGG_DATA(CNT_HCA_ACC=i.Count)
    )

session.commit()






