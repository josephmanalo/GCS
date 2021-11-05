from models import *
from sqlalchemy.sql import func
from traceback import print_exc

def TotalAccount_ForTermination(session):
    """
    Count of account from 'PCC_Account' where collection status = 'Termination Requested'.
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """
    
    try:
        gbu_data = (
            session.query(
                PCC_Account.GBU_ID, func.count(PCC_Account.collection_status).label("Count")
            )
            .filter(PCC_Account.collection_status == "Termination Requested")
            .group_by(PCC_Account.GBU_ID, PCC_Account.collection_status)
        )

        for i in gbu_data:
            session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, TOT_ACC_FORTERM=i.Count))

    except Exception:
        print_exc()


    try:
        grp_data = (
            session.query(
                PCC_Account.Group_ID,
                func.count(PCC_Account.collection_status).label("Count"),
            )
            .filter(PCC_Account.collection_status == "Termination Requested")
            .group_by(PCC_Account.Group_ID, PCC_Account.collection_status)
        )

        for i in grp_data:
            session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, TOT_ACC_FORTERM=i.Count))


    except Exception:
        print_exc()


    try:
        leg_data = (
            session.query(
                PCC_Account.Entity_ID,
                func.count(PCC_Account.collection_status).label("Count"),
            )
            .filter(PCC_Account.collection_status == "Termination Requested")
            .group_by(PCC_Account.Entity_ID, PCC_Account.collection_status)
        )

        for i in leg_data:
            session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, TOT_ACC_FORTERM=i.Count))

    except Exception:
        print_exc()


    try:
        crp_data = (
            session.query(
                PCC_Account.Corp_ID,
                func.count(PCC_Account.collection_status).label("Count"),
            )
            .filter(PCC_Account.collection_status == "Termination Requested")
            .group_by(PCC_Account.Corp_ID, PCC_Account.collection_status)
        )

        for i in crp_data:
            session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, TOT_ACC_FORTERM=i.Count))

    except Exception:
        print_exc()


    try:
        cst_data = (
            session.query(
                PCC_Account.Customer_ID,
                func.count(PCC_Account.collection_status).label("Count"),
            )
            .filter(PCC_Account.collection_status == "Termination Requested")
            .group_by(PCC_Account.Customer_ID, PCC_Account.collection_status)
        )

        for i in cst_data:
            session.add(
                PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, TOT_ACC_FORTERM=i.Count))
        
    except Exception:
        print_exc()

    session.commit()
