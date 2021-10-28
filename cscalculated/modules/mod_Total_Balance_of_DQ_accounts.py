from models import *
from sqlalchemy.sql import func

def Total_Balance_of_DQ_accounts(session):
    """
    Sum of outstanding amount of delinquent Accounts: Check logic for '4. Count_DQAccounts' for identifying 
    the delinquent accounts.
    Sum the outstanding from PCC_Account.OUTSTANDING_BALANCE.
    """

    gbu_data = (
        session.query(
            PCC_Account.GBU_ID,
            func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
        )
        .where(PCC_Account.delinquent_account == "Y")
        .group_by(PCC_Account.GBU_ID)
    )

    grp_data = (
        session.query(
            PCC_Account.Group_ID,
            func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
        )
        .where(PCC_Account.delinquent_account == "Y")
        .group_by(PCC_Account.Group_ID)
    )

    leg_data = (
        session.query(
            PCC_Account.Entity_ID,
            func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
        )
        .where(PCC_Account.delinquent_account == "Y")
        .group_by(PCC_Account.Entity_ID)
    )

    crp_data = (
        session.query(
            PCC_Account.Corp_ID,
            func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
        )
        .where(PCC_Account.delinquent_account == "Y")
        .group_by(PCC_Account.Corp_ID)
    )

    cst_data = (
        session.query(
            PCC_Account.Customer_ID,
            func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
        )
        .where(PCC_Account.delinquent_account == "Y")
        .group_by(PCC_Account.Customer_ID)
    )


    for i in gbu_data:
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance))

    for i in grp_data:
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance))

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID,TOT_BAL_DQ_ACC=i.sum_oustanding_balance))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, TOT_BAL_DQ_ACC=i.sum_oustanding_balance))

    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID,TOT_BAL_DQ_ACC=i.sum_oustanding_balance))


    session.commit()
