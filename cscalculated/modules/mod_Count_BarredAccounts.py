from models import *
from sqlalchemy.sql import func

def Count_BarredAccounts(session):
    """
    Count of account from 'PCC_Account' where collection status = 'Barred'. 
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """

    gbu_data = (
        session.query(
            PCC_Account.GBU_ID,
            func.count(PCC_Account.collection_status).label("Count")
        )
        .filter(PCC_Account.collection_status == 'Barred')
        .group_by(PCC_Account.GBU_ID, PCC_Account.collection_status)
    )

    grp_data = (
            session.query(
            PCC_Account.Group_ID,
            func.count(PCC_Account.collection_status).label("Count")
        )
        .filter(PCC_Account.collection_status == 'Barred')
        .group_by(PCC_Account.Group_ID, PCC_Account.collection_status)
    )

    leg_data = (
        session.query(
            PCC_Account.Entity_ID,
            func.count(PCC_Account.collection_status).label("Count")
        )
        .filter(PCC_Account.collection_status == 'Barred')
        .group_by(PCC_Account.Entity_ID, PCC_Account.collection_status)
    )

    crp_data = (
        session.query(
            PCC_Account.Corp_ID,
            func.count(PCC_Account.collection_status).label("Count")
        )
        .filter(PCC_Account.collection_status == 'Barred')
        .group_by(PCC_Account.Corp_ID, PCC_Account.collection_status)
    )

    cst_data = (
        session.query(
            PCC_Account.Customer_ID,
            func.count(PCC_Account.collection_status).label("Count")
        )
        .filter(PCC_Account.collection_status == 'Barred')
        .group_by(PCC_Account.Customer_ID, PCC_Account.collection_status)
    )

    for i in gbu_data:
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, CNT_BARRED_ACC=i.Count))

    for i in grp_data:
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, CNT_BARRED_ACC=i.Count))

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_BARRED_ACC=i.Count))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_BARRED_ACC=i.Count))
        
    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_BARRED_ACC=i.Count))

    session.commit()





