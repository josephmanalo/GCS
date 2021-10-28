"""
Count of subscriptions where PCC_Subscription where  PCC_Account.collection status = 'For Termination'. 
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


GBU_Count_ForTermination_Subsc = session.query(
    PCC_Subscription.GBU_ID,
    func.count(PCC_Account.collection_status).label("Count"),
).filter(PCC_Account.collection_status == "For Termination").group_by(PCC_Subscription.GBU_ID, PCC_Account.collection_status)

Group_Count_ForTermination_Subsc = session.query(
    PCC_Subscription.Group_ID,
    func.count(PCC_Account.collection_status).label("Count"),
).filter(PCC_Account.collection_status == "For Termination").group_by(PCC_Subscription.Group_ID, PCC_Account.collection_status)

Entity_Count_ForTermination_Subsc = session.query(
    PCC_Subscription.Entity_ID,
    func.count(PCC_Account.collection_status).label("Count"),
).filter(PCC_Account.collection_status == "For Termination").group_by(PCC_Subscription.Entity_ID, PCC_Account.collection_status)

Corp_Count_ForTermination_Subsc = session.query(
    PCC_Subscription.Corp_ID,
    func.count(PCC_Account.collection_status).label("Count"),
).filter(PCC_Account.collection_status == "For Termination").group_by(PCC_Subscription.Corp_ID, PCC_Account.collection_status)

Cust_Count_ForTermination_Subsc = session.query(
    PCC_Subscription.Customer_ID,
    func.count(PCC_Account.collection_status).label("Count"),
).filter(PCC_Account.collection_status == "For Termination").group_by(PCC_Subscription.Customer_ID, PCC_Account.collection_status)


# Insert into DB
for i in GBU_Count_ForTermination_Subsc:
    session.add(
        PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, CNT_SUBSCFORTERM_CC=i.Count)
    )

for i in Group_Count_ForTermination_Subsc:
    session.add(
        PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, CNT_SUBSCFORTERM_CC=i.Count)
    )

for i in Entity_Count_ForTermination_Subsc:
    session.add(
        PCC_ENTITYAGG_DATA(
            Entity_ID=i.Entity_ID, CNT_SUBSCFORTERM_CC=i.Count
        )
    )

for i in Corp_Count_ForTermination_Subsc:
    session.add(
        PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_SUBSCFORTERM_CC=i.Count)
    )

for i in Cust_Count_ForTermination_Subsc:
    session.add(
        PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_SUBSCFORTERM_CC=i.Count)
    )


session.commit()
