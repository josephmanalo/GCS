"""
Formula: (Sum of outstanding amount of delinquent Accounts/ sum of outstanding amount for all accounts) * 100
'only for accounts where account_status = 'Active'

Sum of outstanding amount of delinquent Accounts: Check logic for '4. Count_DQAccounts' 
for identifying the delinquent accounts.
Sum the outstanding from PCC_Account.OUTSTANDING_BALANCE.

Sum of outstanding amount for all accounts from 'PCC_Account'  
Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
"""

from models import *
from sqlalchemy.sql import func, and_
from sqlalchemy.orm import sessionmaker


db = create_engine("postgresql+psycopg2://postgres:common@localhost:5432/sample")
metadata = MetaData(db)
connection = db.connect()
Session = sessionmaker(db)
session = Session()

GBU_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.GBU_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(
        and_(PCC_Account.on_hold_flag == "Y", PCC_Account.account_status == "Active")
    )
    .group_by(PCC_Account.GBU_ID)
)

Group_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Group_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(
        and_(PCC_Account.on_hold_flag == "Y", PCC_Account.account_status == "Active")
    )
    .group_by(PCC_Account.Group_ID)
)

Entity_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Entity_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(
        and_(PCC_Account.on_hold_flag == "Y", PCC_Account.account_status == "Active")
    )
    .group_by(PCC_Account.Entity_ID)
)

Corp_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Corp_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(
        and_(PCC_Account.on_hold_flag == "Y", PCC_Account.account_status == "Active")
    )
    .group_by(PCC_Account.Corp_ID)
)

Cust_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Customer_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(
        and_(PCC_Account.on_hold_flag == "Y", PCC_Account.account_status == "Active")
    )
    .group_by(PCC_Account.Customer_ID)
)

Sum_Balance_of_Accounts = session.query(
    func.sum(PCC_Account.outstanding_balance)
).where(PCC_Account.account_status == "Active")[0][0]


GBU_AR_DQRate = (GBU_Balance_DQ_Accounts / Sum_Balance_of_Accounts) * 100
Group_AR_DQRate = (Group_Balance_DQ_Accounts / Sum_Balance_of_Accounts) * 100
Entity_AR_DQRate = (Entity_Balance_DQ_Accounts / Sum_Balance_of_Accounts) * 100
Corp_AR_DQRate = (Corp_Balance_DQ_Accounts / Sum_Balance_of_Accounts) * 100
Cust_AR_DQRate = (Cust_Balance_DQ_Accounts / Sum_Balance_of_Accounts) * 100


# Insert into DB
for i in GBU_Balance_DQ_Accounts:
    session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, ACTIVE_AR_DQRATE=GBU_AR_DQRate))

for i in Group_Balance_DQ_Accounts:
    session.add(
        PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, ACTIVE_AR_DQRATE=Group_AR_DQRate)
    )

for i in Entity_Balance_DQ_Accounts:
    session.add(
        PCC_ENTITYAGG_DATA(
            Entity_ID=i.Entity_ID, ACTIVE_AR_DQRATE=Entity_AR_DQRate
        )
    )

for i in Corp_Balance_DQ_Accounts:
    session.add(
        PCC_CORPAGG_DATA(
            Customer_ID=i.Customer_ID, ACTIVE_AR_DQRATE=Corp_AR_DQRate
        )
    )

for i in Cust_Balance_DQ_Accounts:
    session.add(
        PCC_CUSTAGG_DATA(GBU_ID=i.GBU_ID, ACTIVE_AR_DQRATE=Cust_AR_DQRate)
    )


session.commit()
