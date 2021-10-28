"""
count of subscriptions where PCC_Subscription.SUBSCRIPTION_STATUS = 'Active' for 
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




#PCC_Subscription.GBU
GBU_Subs_Stats = session.query(
    PCC_Subscription.GBU,
    func.count(PCC_Subscription.subscription_status).label("Count")
).filter(PCC_Subscription.subscription_status == 'Active').group_by(PCC_Subscription.GBU, PCC_Subscription.subscription_status)

#PCC_Subscription.Custom_Group
Custom_Group_Subs_Stats = session.query(
    PCC_Subscription.Custom_Group,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Subscription.subscription_status == 'Active').group_by(PCC_Subscription.Custom_Group, PCC_Subscription.subscription_status)

#PCC_Subscription.Legal_Entitiy
Legal_Entity_Subs_Stats = session.query(
    PCC_Subscription.Legal_Entity,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Subscription.subscription_status == 'Active').group_by(PCC_Subscription.Legal_Entity, PCC_Subscription.subscription_status)

#PCC_Subscription.Corp_ID
Corp_ID_Subs_Stats = session.query(
    PCC_Subscription.Corp_ID,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Subscription.subscription_status == 'Active').group_by(PCC_Subscription.Corp_ID, PCC_Subscription.subscription_status)

#PCC_Subscription.Customer_Id
Customer_ID_Subs_Stats = session.query(
    PCC_Subscription.Customer_ID,
    func.count(PCC_Exception.vip_flag).label("Count")
).filter(PCC_Subscription.subscription_status == 'Active').group_by(PCC_Subscription.Customer_ID, PCC_Subscription.subscription_status)




# Insert into DB 
#PrimaryKey is Auto Increment

for i in GBU_Subs_Stats:
    session.add(
        PCC_GBUAGG_DATA(CNT_ACTIVESUBSC_CC=i.Count)
    )

for i in Custom_Group_Subs_Stats:
    session.add(
        PCC_COMPGAGG_DATA(CNT_ACTIVESUBSC_CC=i.Count)
    )

for i in Legal_Entity_Subs_Stats:
    session.add(
        PCC_ENTITYAGG_DATA(CNT_ACTIVESUBSC_CC=i.Count)
    )

for i in Corp_ID_Subs_Stats:
    session.add(
        PCC_CORPAGG_DATA(CNT_ACTIVESUBSC_CC=i.Count)
    )
    
for i in Customer_ID_Subs_Stats:
    session.add(
        PCC_CUSTAGG_DATA(CNT_ACTIVESUBSC_CC=i.Count)
    )

session.commit()






