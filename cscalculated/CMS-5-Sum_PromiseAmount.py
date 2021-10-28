"""
SUM(PCC_ARRANGEMENT.PROMISE_AMOUNT) WHERE ISONARRANGEMENT = 'Y'
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



GBU_PromiseAmnt = session.query(
    PCC_Arrangement.GBU,
    func.count(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
).where(PCC_Arrangement.isonarrangement == 'Y').group_by(PCC_Arrangement.GBU, PCC_Arrangement.isonarrangement)

Custom_Group_PromiseAmnt = session.query(
    PCC_Arrangement.Custom_Group,
    func.count(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
).where(PCC_Arrangement.isonarrangement == 'Y').group_by(PCC_Arrangement.Custom_Group, PCC_Arrangement.isonarrangement)

Legal_Entity_PromiseAmnt = session.query(
    PCC_Arrangement.Legal_Entity,
    func.count(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
).where(PCC_Arrangement.isonarrangement == 'Y').group_by(PCC_Arrangement.Legal_Entity, PCC_Arrangement.isonarrangement)

Corp_ID_PromiseAmnt = session.query(
    PCC_Arrangement.Corp_ID,
    func.count(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
).where(PCC_Arrangement.isonarrangement == 'Y').group_by(PCC_Arrangement.Corp_ID, PCC_Arrangement.isonarrangement)

Customer_ID_PromiseAmnt = session.query(
    PCC_Arrangement.Customer_ID,
    func.count(PCC_Arrangement.promise_amount).label("sum_promise_amount"),
).where(PCC_Arrangement.isonarrangement == 'Y').group_by(PCC_Arrangement.Customer_ID, PCC_Arrangement.isonarrangement)




# Insert into DB 
#PrimaryKey is Auto Increment

for i in GBU_PromiseAmnt:
    session.add(
        PCC_GBUAGG_DATA(SUM_PROMISEAMT_GBU=i.Count)
    )

for i in Custom_Group_PromiseAmnt:
    session.add(
        PCC_COMPGAGG_DATA(SUM_PROMISEAMT_GBU=i.Count)
    )

for i in Legal_Entity_PromiseAmnt:
    session.add(
        PCC_ENTITYAGG_DATA(SUM_PROMISEAMT_GBU=i.Count)
    )

for i in Corp_ID_PromiseAmnt:
    session.add(
        PCC_CORPAGG_DATA(SUM_PROMISEAMT_GBU=i.Count)
    )
    
for i in Customer_ID_PromiseAmnt:
    session.add(
        PCC_CUSTAGG_DATA(SUM_PROMISEAMT_GBU=i.Count)
    )

session.commit()





