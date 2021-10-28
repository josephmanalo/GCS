"""
count of accounts where PCC_EXCEPTION.VIP_FLAG = 'Y', 
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




#PCC_Exception.GBU
GBU_VIP = session.query(
    PCC_Exception.GBU,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Exception.vip_flag == 'Y').group_by(PCC_Exception.GBU, PCC_Exception.vip_flag)

#PCC_Exception.Custom_Group
Custom_Group_VIP = session.query(
    PCC_Exception.Custom_Group,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Exception.vip_flag == 'Y').group_by(PCC_Exception.Custom_Group, PCC_Exception.vip_flag)

#PCC_Exception.Legal_Entitiy
Legal_Entity_VIP = session.query(
    PCC_Exception.Legal_Entity,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Exception.vip_flag == 'Y').group_by(PCC_Exception.Legal_Entity, PCC_Exception.vip_flag)

#PCC_Exception.Corp_ID
Corp_ID_VIP = session.query(
    PCC_Exception.Corp_ID,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Exception.vip_flag == 'Y').group_by(PCC_Exception.Corp_ID, PCC_Exception.vip_flag)

#PCC_Exception.Customer_Id
Customer_ID_VIP = session.query(
    PCC_Exception.Customer_ID,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Exception.vip_flag == 'Y').group_by(PCC_Exception.Customer_ID, PCC_Exception.vip_flag)




# Insert into DB 
#PrimaryKey is Auto Increment

for i in GBU_VIP:
    session.add(
        PCC_GBUAGG_DATA(CNT_VIP=i.Count)
    )

for i in Custom_Group_VIP:
    session.add(
        PCC_COMPGAGG_DATA(CNT_VIP=i.Count)
    )

for i in Legal_Entity_VIP:
    session.add(
        PCC_ENTITYAGG_DATA(CNT_VIP=i.Count)
    )

for i in Corp_ID_VIP:
    session.add(
        PCC_CORPAGG_DATA(CNT_VIP=i.Count)
    )
    
for i in Customer_ID_VIP:
    session.add(
        PCC_CUSTAGG_DATA(CNT_VIP=i.Count)
    )

session.commit()






