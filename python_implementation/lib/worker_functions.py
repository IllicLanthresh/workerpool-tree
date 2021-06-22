def modem_is_super_offline(*args, **child_results):
    antenna_is_tracking = child_results['antenna_is_tracking']
    modem_is_online = child_results['is_modem_online']

    return not (antenna_is_tracking and modem_is_online)
