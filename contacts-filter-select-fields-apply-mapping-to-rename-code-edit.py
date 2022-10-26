import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

num_output_files = 4 ## If loading to Redshift, Number of files created should be equal to the number of slices in the cluster

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://coreprofile-entities/contacts/contacts-flattened/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Filter
Filter_node1666814280676 = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (
        bool(re.match("2~.*", row["item_hash_key_s"]))
        and bool(re.match("v0_contact.*", row["item_range_key_s"]))
    ),
    transformation_ctx="Filter_node1666814280676",
)

# Script generated for node Select Fields
SelectFields_node1666814291059 = SelectFields.apply(
    frame=Filter_node1666814280676,
    paths=[
        "item_contact_types_l_sk",
        "item_notification_subscription_l_sk",
        "item_hash_key_s",
        "item_range_key_s",
        "item_gsi2_hash_key_s",
        "item_gsi2_range_key_s",
        "item_update_user_s",
        "item_gsi1_hash_key_s",
        "item_gsi1_range_key_s",
        "item_update_date_s",
        "item_created_date_s",
        "item_customer_number_s",
        "item_create_user_s",
        "item_gsi5_range_key_s",
        "item_gsi5_hash_key_s",
        "item_customer_name_s",
        "item_bill_to_number_s",
        "item_is_deleted_bool",
        "item_phone_country_code_s",
        "item_is_group_bool",
        "item_preferred_language_s",
        "item_visible_externally_bool",
        "item_contact_level_s",
        "item_gsi4_range_key_s",
        "item_email_address_s",
        "item_email_address_null",
        "item_gsi4_hash_key_s",
        "item_preferred_method_s",
        "item_last_name_s",
        "item_first_name_s",
        "item_gsi3_hash_key_s",
        "item_st_phone_s",
        "item_gsi3_range_key_s",
        "item_added_date_s",
        "item_phone_cntry_code_s",
        "item_parent_company_name_null",
        "item_ship_to_customer_null",
        "item_ship_to_customer_name_null",
        "item_home_office_name_s",
        "item_phone_number_s",
        "item_job_title_s",
        "item_notification_subscription_l_m_cc_emails_l_sk",
        "item_notification_subscription_l_m_method_l_sk",
        "m_id_s",
        "m_event_s",
        "m_type_s",
        "s",
        "item_notification_subscription_l_m_cc_emails_l_s",
        "item_contact_types_l_m_role_area_l_sk",
        "item_contact_types_l_m_role_reas_l_sk",
        "item_contact_types_l_m_role_areas_l_sk",
        "m_service_s",
        "item_contact_types_l_m_id_s",
        "item_contact_types_l_m_role_areas_l_m_levels_l_sk",
        "m_role_area_s",
        "m_level_s",
        "m_location_s",
        "m_level_null",
        "item_contact_types_l_m_role_reas_l_m_levels_l_sk",
        "item_contact_types_l_m_role_reas_l_m_role_area_s",
        "item_contact_types_l_m_role_reas_l_m_location_s",
        "item_contact_types_l_m_role_area_l_m_levels_l_sk",
        "item_contact_types_l_m_role_area_l_m_role_area_s",
        "item_contact_types_l_m_role_area_l_m_location_s",
    ],
    transformation_ctx="SelectFields_node1666814291059",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=SelectFields_node1666814291059,
    mappings=[
        ("item_contact_types_l_sk", "long", "contact_types_sk", "long"),
        (
            "item_notification_subscription_l_sk",
            "long",
            "notification_subscription_sk",
            "long",
        ),
        ("item_hash_key_s", "string", "hash_key", "string"),
        ("item_range_key_s", "string", "range_key", "string"),
        ("item_gsi2_hash_key_s", "string", "gsi2_hash_key", "string"),
        ("item_gsi2_range_key_s", "string", "gsi2_range_key", "string"),
        ("item_update_user_s", "string", "update_user", "string"),
        ("item_gsi1_hash_key_s", "string", "gsi1_hash_key", "string"),
        ("item_gsi1_range_key_s", "string", "gsi1_range_key", "string"),
        ("item_update_date_s", "string", "update_date", "string"),
        ("item_created_date_s", "string", "created_date", "string"),
        ("item_customer_number_s", "string", "customer_number", "string"),
        ("item_create_user_s", "string", "create_user", "string"),
        ("item_gsi5_range_key_s", "string", "gsi5_range_key", "string"),
        ("item_gsi5_hash_key_s", "string", "gsi5_hash_key", "string"),
        ("item_customer_name_s", "string", "customer_name", "string"),
        ("item_bill_to_number_s", "string", "bill_to_number", "string"),
        ("item_is_deleted_bool", "boolean", "is_deleted", "boolean"),
        ("item_phone_country_code_s", "string", "phone_country_code", "string"),
        ("item_is_group_bool", "boolean", "is_group", "boolean"),
        ("item_preferred_language_s", "string", "preferred_language", "string"),
        ("item_visible_externally_bool", "boolean", "visible_externally", "boolean"),
        ("item_contact_level_s", "string", "contact_level", "string"),
        ("item_gsi4_range_key_s", "string", "gsi4_range_key", "string"),
        ("item_email_address_s", "string", "email_address", "string"),
        ("item_email_address_null", "boolean", "email_address_null", "boolean"),
        ("item_gsi4_hash_key_s", "string", "gsi4_hash_key", "string"),
        ("item_preferred_method_s", "string", "preferred_method", "string"),
        ("item_last_name_s", "string", "last_name", "string"),
        ("item_first_name_s", "string", "first_name", "string"),
        ("item_gsi3_hash_key_s", "string", "gsi3_hash_key", "string"),
        ("item_st_phone_s", "string", "st_phone", "string"),
        ("item_gsi3_range_key_s", "string", "gsi3_range_key", "string"),
        ("item_added_date_s", "string", "added_date", "string"),
        ("item_phone_cntry_code_s", "string", "phone_cntry_code", "string"),
        ("item_parent_company_name_null", "boolean", "parent_company_name", "boolean"),
        ("item_ship_to_customer_null", "boolean", "ship_to_customer", "boolean"),
        (
            "item_ship_to_customer_name_null",
            "boolean",
            "ship_to_customer_name",
            "boolean",
        ),
        ("item_home_office_name_s", "string", "home_office_name", "string"),
        ("item_phone_number_s", "string", "phone_number", "string"),
        ("item_job_title_s", "string", "job_title", "string"),
        (
            "item_notification_subscription_l_m_cc_emails_l_sk",
            "long",
            "notification_subscription_emails_sk",
            "long",
        ),
        (
            "item_notification_subscription_l_m_method_l_sk",
            "long",
            "notification_subscription_method_sk",
            "long",
        ),
        ("m_id_s", "string", "m_id", "string"),
        ("m_event_s", "string", "m_event", "string"),
        ("m_type_s", "string", "m_type", "string"),
        ("s", "string", "s", "string"),
        (
            "item_notification_subscription_l_m_cc_emails_l_s",
            "string",
            "notification_subscription_emails",
            "string",
        ),
        (
            "item_contact_types_l_m_role_area_l_sk",
            "long",
            "contact_types_role_area_sk",
            "long",
        ),
        (
            "item_contact_types_l_m_role_reas_l_sk",
            "long",
            "contact_types_role_reas_sk",
            "long",
        ),
        (
            "item_contact_types_l_m_role_areas_l_sk",
            "long",
            "contact_types_role_areas_sk",
            "long",
        ),
        ("m_service_s", "string", "m_service", "string"),
        ("item_contact_types_l_m_id_s", "string", "contact_types_id", "string"),
        (
            "item_contact_types_l_m_role_areas_l_m_levels_l_sk",
            "long",
            "contact_types_role_areas_levels_sk",
            "long",
        ),
        ("m_role_area_s", "string", "m_role_area", "string"),
        ("m_level_s", "string", "m_level", "string"),
        ("m_location_s", "string", "m_location", "string"),
        ("m_level_null", "boolean", "m_level_null", "boolean"),
        (
            "item_contact_types_l_m_role_reas_l_m_levels_l_sk",
            "long",
            "contact_types_role_reas_levels_l_sk",
            "long",
        ),
        (
            "item_contact_types_l_m_role_reas_l_m_role_area_s",
            "string",
            "contact_types_role_reas_role_area",
            "string",
        ),
        (
            "item_contact_types_l_m_role_reas_l_m_location_s",
            "string",
            "contact_types_role_reas_location",
            "string",
        ),
        (
            "item_contact_types_l_m_role_area_l_m_levels_l_sk",
            "long",
            "contact_types_role_area_levels_sk",
            "long",
        ),
        (
            "item_contact_types_l_m_role_area_l_m_role_area_s",
            "string",
            "contact_types_role_area_role_area",
            "string",
        ),
        (
            "item_contact_types_l_m_role_area_l_m_location_s",
            "string",
            "contact_types_role_area_location",
            "string",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

dyn_frame = ApplyMapping_node2.coalesce(num_output_files)


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=dyn_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://coreprofile-entities/contacts/filter-select-fields-apply-mapping-to-rename/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
