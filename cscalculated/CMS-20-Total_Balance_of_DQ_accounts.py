"""
Sum of outstanding amount of delinquent Accounts: Check logic for '4. Count_DQAccounts' for identifying 
the delinquent accounts.
Sum the outstanding from PCC_Account.OUTSTANDING_BALANCE.
"""

from models import *
from sqlalchemy.sql import func
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
    .where(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.GBU_ID)
)

Group_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Group_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Group_ID)
)

Entity_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Entity_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Entity_ID)
)

Corp_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Corp_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Corp_ID)
)

Cust_Balance_DQ_Accounts = (
    session.query(
        PCC_Account.Customer_ID,
        func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
    )
    .where(PCC_Account.on_hold_flag == "Y")
    .group_by(PCC_Account.Customer_ID)
)


# Insert into DB
for i in GBU_Balance_DQ_Accounts:
    session.add(
        PCC_GBUAGG_DATA(
            GBU_ID=i.GBU_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance
        )
    )

for i in Group_Balance_DQ_Accounts:
    session.add(
        PCC_COMPGAGG_DATA(
            Group_ID=i.Group_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance
        )
    )

for i in Entity_Balance_DQ_Accounts:
    session.add(
        PCC_ENTITYAGG_DATA(
            Entity_ID=i.Entity_ID,
            TOT_BAL_DQ_ACC=i.sum_oustanding_balance,
        )
    )

for i in Corp_Balance_DQ_Accounts:
    session.add(
        PCC_CORPAGG_DATA(
            Corp_ID=i.Corp_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance
        )
    )

for i in Cust_Balance_DQ_Accounts:
    session.add(
        PCC_CUSTAGG_DATA(
            Customer_ID=i.Customer_ID,
            TOT_BAL_DQ_ACC=i.sum_oustanding_balance,
        )
    )


session.commit()
