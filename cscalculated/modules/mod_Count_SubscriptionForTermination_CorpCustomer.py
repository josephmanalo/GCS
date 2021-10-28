from models import *
from sqlalchemy.sql import func


def Count_SubscriptionForTermination_CorpCustomer(session):
    """
    Count of subscriptions where PCC_Subscription where  PCC_Account.collection status = 'For Termination'.
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table."
    """
    
    gbu_data = (
        session.query(
            PCC_Subscription.GBU_ID,
            func.count(PCC_Account.collection_status).label("Count"),
        ).join(PCC_Account,
            PCC_Account.GBU_ID == PCC_Subscription.GBU_ID
            )
        .filter(PCC_Account.collection_status == "For Termination")
        .group_by(PCC_Subscription.GBU_ID, PCC_Account.collection_status)
    )

    grp_data = (
        session.query(
            PCC_Subscription.Group_ID,
            func.count(PCC_Account.collection_status).label("Count"),
        ).join(PCC_Account,
            PCC_Account.Group_ID == PCC_Subscription.Group_ID
            )
        .filter(PCC_Account.collection_status == "For Termination")
        .group_by(PCC_Subscription.Group_ID, PCC_Account.collection_status)
    )

    leg_data = (
        session.query(
            PCC_Subscription.Entity_ID,
            func.count(PCC_Account.collection_status).label("Count"),
        ).join(PCC_Account,
            PCC_Account.Entity_ID == PCC_Subscription.Entity_ID
            )
        .filter(PCC_Account.collection_status == "For Termination")
        .group_by(PCC_Subscription.Entity_ID, PCC_Account.collection_status)
    )

    crp_data = (
        session.query(
            PCC_Subscription.Corp_ID,
            func.count(PCC_Account.collection_status).label("Count"),
        ).join(PCC_Account,
            PCC_Account.Corp_ID == PCC_Subscription.Corp_ID
            )
        .filter(PCC_Account.collection_status == "For Termination")
        .group_by(PCC_Subscription.Corp_ID, PCC_Account.collection_status)
    )

    cst_data = (
        session.query(
            PCC_Subscription.Customer_ID,
            func.count(PCC_Account.collection_status).label("Count"),
        ).join(PCC_Account,
            PCC_Account.Customer_ID == PCC_Subscription.Customer_ID
            )
        .filter(PCC_Account.collection_status == "For Termination")
        .group_by(PCC_Subscription.Customer_ID, PCC_Account.collection_status)
    )

    for i in gbu_data:
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, CNT_SUBSCFORTERM_CC=i.Count))

    for i in grp_data:
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, CNT_SUBSCFORTERM_CC=i.Count))

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_SUBSCFORTERM_CC=i.Count))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_SUBSCFORTERM_CC=i.Count))

    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_SUBSCFORTERM_CC=i.Count))

    session.commit()
