"""
Count of account from 'PCC_Account' where ON_HOLD_FLAG = 'Y'. 
Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

Formula: (Number of delinquent Accounts/ total number of accounts) * 100
Number of delinquent accounts: Check logic for '4. Count_DQAccounts'.
Total number of accounts: Count of account from 'PCC_Account'  
Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

"""

from models import *
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker


db = create_engine("postgresql+psycopg2://postgres:common@localhost:5432/sample")
metadata = MetaData(db)
connection = db.connect()
Session = sessionmaker(db)
session = Session()

GBU_Count_DQ_Accounts = (
    session.query(
        PCC_Account.GBU_ID, func.count(PCC_Account.on_hold_flag).label("Count")
    )
    .filter(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.GBU_ID, PCC_Account.on_hold_flag)
)

Group_Count_DQ_Accounts = (
    session.query(
        PCC_Account.Group_ID, func.count(PCC_Account.on_hold_flag).label("Count")
    )
    .filter(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Group_ID, PCC_Account.on_hold_flag)
)

Entity_Count_DQ_Accounts = (
    session.query(
        PCC_Account.Entity_ID, func.count(PCC_Account.on_hold_flag).label("Count")
    )
    .filter(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Entity_ID, PCC_Account.on_hold_flag)
)

Corp_Count_DQ_Accounts = (
    session.query(
        PCC_Account.Corp_ID, func.count(PCC_Account.on_hold_flag).label("Count")
    )
    .filter(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Corp_ID, PCC_Account.on_hold_flag)
)

Cust_Count_DQ_Accounts = (
    session.query(
        PCC_Account.Customer_ID, func.count(PCC_Account.on_hold_flag).label("Count")
    )
    .filter(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Customer_ID, PCC_Account.on_hold_flag)
)

Total_No_of_Accounts = session.query(PCC_Account).count()

GBU_DQ_Rate = (GBU_Count_DQ_Accounts / Total_No_of_Accounts) * 100
Group_DQ_Rate = (Group_Count_DQ_Accounts / Total_No_of_Accounts) * 100
Entity_DQ_Rate = (Entity_Count_DQ_Accounts / Total_No_of_Accounts) * 100
Corp_DQ_Rate = (Corp_Count_DQ_Accounts / Total_No_of_Accounts) * 100
Cust_DQ_Rate = (Cust_Count_DQ_Accounts / Total_No_of_Accounts) * 100


# Insert into DB
for i in GBU_Count_DQ_Accounts:
    session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, ACCT_DQRATE=GBU_DQ_Rate))

for i in Group_Count_DQ_Accounts:
    session.add(
        PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, ACCT_DQRATE=Group_DQ_Rate)
    )

for i in Entity_Count_DQ_Accounts:
    session.add(
        PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, ACCT_DQRATE=Entity_DQ_Rate)
    )

for i in Corp_Count_DQ_Accounts:
    session.add(
        PCC_CORPAGG_DATA(Customer_ID=i.Customer_ID, ACCT_DQRATE=Corp_DQ_Rate)
    )

for i in Cust_Count_DQ_Accounts:
    session.add(PCC_CUSTAGG_DATA(GBU_ID=i.GBU_ID, ACCT_DQRATE=Cust_DQ_Rate))


session.commit()
