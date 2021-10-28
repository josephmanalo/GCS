from models import *
from sqlalchemy.sql import func

def Total_AR_DQ_Rate(session):
    """
    Count of account from 'PCC_Account' where delinquent_account = 'Y'. 
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

    Formula: (Sum of outstanding amount of delinquent Accounts/ sum of outstanding amount for all accounts) * 100
    Sum of outstanding amount of delinquent Accounts: Check logic for '4. Count_DQAccounts' 
    for identifying the delinquent accounts.
    Sum the outstanding from PCC_Account.OUTSTANDING_BALANCE.

    Sum of outstanding amount for all accounts from 'PCC_Account'  
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
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

    Sum_Balance_of_Accounts = session.query(func.sum(PCC_Account.outstanding_balance).label('total_sum')).first()['total_sum']
    
    for i in gbu_data:
        GBU_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, TOT_AR_DQRATE=GBU_AR_DQRate))

    for i in grp_data:
        Group_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, TOT_AR_DQRATE=Group_AR_DQRate))

    for i in leg_data:
        Entity_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, TOT_AR_DQRATE=Entity_AR_DQRate))

    for i in crp_data:
        Corp_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, TOT_AR_DQRATE=Corp_AR_DQRate))

    for i in cst_data:
        Cust_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, TOT_AR_DQRATE=Cust_AR_DQRate))

    session.commit()
