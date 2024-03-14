with source as (
    select
        b.Borrower_id as Borrower_id,
        b.Name as BorrowerName,
        b."city" as BorrowerCity,
        b."Gender" as Gender,
        b."Race" as Race,
        b."Veteran" as VeteranStatus,
        b.address as BorrowerAddress,
        b."state" as "State" ,
        b."ProjectCity",
        b."ProjectState",
        b."RuralUrbanIndicator",
        l.Lender_id,
        l.Name as ServicingLenderName,
        l.city as ServicingLenderCity,
        l."office_code" as SBAOfficeCode,
        l.state as ServicingLenderState,
        lo."id" as loanstatus_id,
        Lo."LoanStatus",
        bage."id" as businessage_id,
        bage."BusinessAgeDescription",
        btype."type_id" as businesstype_id,
        btype."BusinessType",
        pm."id" as processingmethod_id,
        pm."ProcessingMethod" ,
        f."id",
        f.Term,
        f.DateApproved,
        f.DateForgived,
        f.SBAGuarantyPercentage,
        f.InitialApprovalAmount,
        f.CurrentApprovalAmount,
        f.UndisbursedAmount,
        f.JobsReported,
        f.ForgivenessAmount,
        current_timestamp as insertion_timestamp
    from {{ ref('fact') }} f
    left join {{ ref('borrowers') }} b
    on b.Borrower_id = f."id"
    left join {{ ref('lenders') }} l
    on l.Lender_id = f.Lender_id
    left join {{ ref('loanstatus') }} lo
    on lo.id = f."loanstatus_id"
    left join {{ ref('businessage') }} bage
    on bage.id = f."businessage_id"
    left join {{ ref('businesstype') }} btype
    on btype.type_id = f."businesstype_id"
    left join {{ ref('processingmethod') }} pm
    on pm.id = f."processingmethod_id"
)
select * 
from source