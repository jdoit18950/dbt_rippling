with 

source as (

    select * from {{ source('ringcentral', 'account_call_log_leg') }}

),

renamed as (

    select
        account_call_log_id,
        index,
        start_time,
        duration,
        duration_ms,
        type,
        internal_type,
        direction,
        action,
        result,
        telephony_session_id,
        sip_uuid_info,
        party_id,
        transport,
        short_recording,
        reason,
        reason_description,
        transfer_target_telephony_session_id,
        transferee_telephony_session_id,
        billing_cost_included,
        billing_cost_purchased,
        message_id,
        delegate_id,
        delegate_name,
        recording_id,
        recording_type,
        leg_type,
        master,
        from_phone_number,
        from_extension_number,
        from_extension_id,
        from_location,
        from_name,
        from_dialed_phone_number,
        from_device_id,
        to_phone_number,
        to_extension_number,
        to_extension_id,
        to_location,
        to_name,
        to_dialed_phone_number,
        to_device_id,
        extension_id,
        _fivetran_synced

    from source

)

select * from renamed
