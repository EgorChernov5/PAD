table_cdn_plans = "cdn.plans"
table_cdn_editors = "cdn.editors"
table_dds_plans = "dds.plans"
table_dds_editors = "dds.editors"

drop table if exists cdn.plans_with_doubles;
    create table cdn.plans_with_doubles as (
        select
            *
        from
            dds.plans
        order by
            source_id,
            entry_date desc
    );

drop table if exists cdn.plans_actual;
create table cdn.plans_actual as (
    select
        distinct on(source_id) source_id,
        educational_profile ,
        number ,
        approval_date ,
        year ,
        education_form ,
        qualification ,
        on_check ,
        laboriousness ,
        rating ,
        entry_date ,
        is_deleted,
        name_op ,
        code_op ,
        faculty_op
    from
        table_dds_plans
    order by
        source_id,
        entry_date desc
);

drop table if exists {table_cdn_editors};
create table {table_cdn_editors} as (
    select
        distinct on(source_id) source_id,
        id_editor ,
        user_name ,
        first_name ,
        last_name ,
        email ,
        isu_number ,
        entry_date
    from
        table_dds_editors
    order by
        source_id,
        entry_date desc
);


drop table if exists cdn.plans_with_editors;
create table cdn.plans_with_editors as (
select
    t_1.source_id as editor_source_id,
    t_1.id_editor,
    t_1.user_name,
    t_1.first_name,
    t_1.last_name,
    t_1.email,
    t_1.isu_number,
    t_2.year,
    t_2.qualification,
    t_2.is_deleted,
    t_2.name_op,
    t_2.code_op,
    t_2.faculty_op,
    t_2.laboriousness,
    t_2.on_check,
    t_2.entry_date
from
    {table_cdn_editors} t_1
left join lateral (
    select *
    from {table_cdn_plans} t
    where t_1.source_id = t.source_id
    order by t.entry_date desc
    limit 1
) t_2 on true
order by
    t_1.source_id, t_2.entry_date desc
);


drop table if exists cdn.disciplines_with_plans;
create table cdn.plans_with_editors as (
    select



);

