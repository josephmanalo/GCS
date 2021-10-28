"""
Count of subscriptions where PCC_Subscription.SUBSCRIPTION_STATUS = 'Terminated'
Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table."
"""

from models import *
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker


db = create_engine("postgresql+psycopg2://postgres:common@localhost:5432/sample")
metadata = MetaData(db)
connection = db.connect()
Session = sessionmaker(db)
session = Session()

Entity_Count_Terminated_Subsc = session.query(
    PCC_Subscription.Entity_ID,
    func.count(PCC_Subscription.subscription_status).label("Count"),
).filter(PCC_Subscription.subscription_status == "Terminated").group_by(PCC_Subscription.Entity_ID, PCC_Subscription.subscription_status)

Corp_Count_Terminated_Subsc = session.query(
    PCC_Subscription.Corp_ID,
    func.count(PCC_Subscription.subscription_status).label("Count"),
).filter(PCC_Subscription.subscription_status == "Terminated").group_by(PCC_Subscription.Corp_ID, PCC_Subscription.subscription_status)

Cust_Count_Terminated_Subsc = session.query(
    PCC_Subscription.Customer_ID,
    func.count(PCC_Subscription.subscription_status).label("Count"),
).filter(PCC_Subscription.subscription_status == "Terminated").group_by(PCC_Subscription.Customer_ID, PCC_Subscription.subscription_status)


# Insert into DB
for i in Entity_Count_Terminated_Subsc:
    session.add(
        PCC_ENTITYAGG_DATA(
            Entity_ID=i.Entity_ID, CNT_TERMSUBSC_IC=i.Count
        )
    )

for i in Corp_Count_Terminated_Subsc:
    session.add(
        PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_TERMSUBSC_IC=i.Count)
    )

for i in Cust_Count_Terminated_Subsc:
    session.add(
        PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_TERMSUBSC_IC=i.Count)
    )


session.commit()
