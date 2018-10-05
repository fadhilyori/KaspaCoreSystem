package me.mamotis.kaspacore.util

object Commons {
  case class EventObj(company: String, device_id: String, year: Integer, month: Integer,
                      day: Integer, hour: Integer, minute: Integer,
                      second: Integer, protocol: String, ip_type: String,
                      src_mac: String, dest_mac: String, src_ip: String,
                      dest_ip: String, src_port: Integer, dest_port: Integer,
                      alert_msg: String, classification: Integer,
                      priority: Integer, sig_id: Integer, sig_gen: Integer,
                      sig_rev: Integer, src_country: String, src_region: String,
                      dest_country: String, dest_region: String) extends Serializable

  case class EventObjTs(company: String, device_id: String, timestamp: String, protocol: String, ip_type: String,
                        src_mac: String, dest_mac: String, src_ip: String,
                        dest_ip: String, src_port: Integer, dest_port: Integer,
                        alert_msg: String, classification: Integer,
                        priority: Integer, sig_id: Integer, sig_gen: Integer,
                        sig_rev: Integer, src_country: String, src_region: String,
                        dest_country: String, dest_region: String) extends Serializable

  case class EventHitCompanyObjSecond(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                   minute: Integer, second: Integer, value: Long) extends Serializable

  case class EventHitCompanyObjMin(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                   minute: Integer, value: Long) extends Serializable

  case class EventHitCompanyObjHour(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    value: Long) extends Serializable

  case class EventHitCompanyObjDay(company: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjSecond(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, second: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjMin(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjHour(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                     value: Long) extends Serializable

  case class EventHitDeviceIdObjDay(device_id: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  //  Signature Related Obj

  case class SignatureHitCompanyObjSecond(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, second: Integer, value: Long) extends Serializable

  case class SignatureHitCompanyObjMin(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, value: Long) extends Serializable

  case class SignatureHitCompanyObjHour(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        value: Long) extends Serializable

  case class SignatureHitCompanyObjDay(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjSecond(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, second: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjMin(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjHour(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         value: Long) extends Serializable

  case class SignatureHitDeviceIdObjDay(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  //  Protocol Related Obj

  case class ProtocolHitCompanyObjSecond(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolHitCompanyObjMin(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, value: Long) extends Serializable

  case class ProtocolHitCompanyObjHour(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       value: Long) extends Serializable

  case class ProtocolHitCompanyObjDay(company: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjSecond(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjMin(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjHour(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjDay(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  //  Protocol + Port Related Obj

  case class ProtocolBySPortHitCompanyObjSecond(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjMin(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjHour(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjDay(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjSecond(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjMin(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjHour(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                               value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjDay(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjSecond(company: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjMin(company: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjHour(company: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjDay(company: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjSecond(device_id: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjMin(device_id: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjHour(device_id: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                               value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjDay(device_id: String, protocol: String, dest_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  //  IP + Country Related Obj

  case class IPSourceHitCompanyObjSecond(company: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, second: Integer, value: Long) extends Serializable

  case class IPSourceHitCompanyObjMin(company: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, value: Long) extends Serializable

  case class IPSourceHitCompanyObjHour(company: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       value: Long) extends Serializable

  case class IPSourceHitCompanyObjDay(company: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class IPSourceHitDeviceIdObjSecond(device_id: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, second: Integer, value: Long) extends Serializable

  case class IPSourceHitDeviceIdObjMin(device_id: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, value: Long) extends Serializable

  case class IPSourceHitDeviceIdObjHour(device_id: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        value: Long) extends Serializable

  case class IPSourceHitDeviceIdObjDay(device_id: String, src_ip: String, country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class IPDestHitCompanyObjSecond(company: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, second: Integer, value: Long) extends Serializable

  case class IPDestHitCompanyObjMin(company: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, value: Long) extends Serializable

  case class IPDestHitCompanyObjHour(company: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                     value: Long) extends Serializable

  case class IPDestHitCompanyObjDay(company: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class IPDestHitDeviceIdObjSecond(device_id: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                     minute: Integer, second: Integer, value: Long) extends Serializable

  case class IPDestHitDeviceIdObjMin(device_id: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                     minute: Integer, value: Long) extends Serializable

  case class IPDestHitDeviceIdObjHour(device_id: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      value: Long) extends Serializable

  case class IPDestHitDeviceIdObjDay(device_id: String, dest_ip: String, country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class CountrySrcHitCompanyObjSecond(company: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, second: Integer, value: Long) extends Serializable

  case class CountrySrcHitCompanyObjMin(company: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, value: Long) extends Serializable

  case class CountrySrcHitCompanyObjHour(company: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         value: Long) extends Serializable

  case class CountrySrcHitCompanyObjDay(company: String, src_country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class CountrySrcHitDeviceIdObjSecond(device_id: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         minute: Integer, second: Integer, value: Long) extends Serializable

  case class CountrySrcHitDeviceIdObjMin(device_id: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         minute: Integer, value: Long) extends Serializable

  case class CountrySrcHitDeviceIdObjHour(device_id: String, src_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                          value: Long) extends Serializable

  case class CountrySrcHitDeviceIdObjDay(device_id: String, src_country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class CountryDestHitCompanyObjSecond(company: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         minute: Integer, second: Integer, value: Long) extends Serializable

  case class CountryDestHitCompanyObjMin(company: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         minute: Integer, value: Long) extends Serializable

  case class CountryDestHitCompanyObjHour(company: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                          value: Long) extends Serializable

  case class CountryDestHitCompanyObjDay(company: String, dest_country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class CountryDestHitDeviceIdObjSecond(device_id: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                          minute: Integer, second: Integer, value: Long) extends Serializable

  case class CountryDestHitDeviceIdObjMin(device_id: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                          minute: Integer, value: Long) extends Serializable

  case class CountryDestHitDeviceIdObjHour(device_id: String, dest_country: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                           value: Long) extends Serializable

  case class CountryDestHitDeviceIdObjDay(device_id: String, dest_country: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

}
