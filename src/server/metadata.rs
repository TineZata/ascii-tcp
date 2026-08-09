use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlarmSeverity {
    #[default]
    None,
    Minor,
    Major,
    Invalid,
    Undefined,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlarmStatus {
    #[default]
    NoAlarm,
    Device,
    Driver,
    Record,
    Database,
    Config,
    Undefined,
    Client,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ServerMetadata {
    #[serde(rename = "alarm_serverity", alias = "alarm_severity")]
    pub alarm_severity: AlarmSeverity,
    pub alarm_status: AlarmStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_backend_server_alarm_metadata() {
        let metadata: ServerMetadata =
            serde_json::from_str(r#"{"alarm_serverity":"NONE","alarm_status":"NO_ALARM"}"#)
                .unwrap();

        assert_eq!(metadata.alarm_severity, AlarmSeverity::None);
        assert_eq!(metadata.alarm_status, AlarmStatus::NoAlarm);
    }

    #[test]
    fn accepts_correctly_spelled_alarm_severity_alias() {
        let metadata: ServerMetadata =
            serde_json::from_str(r#"{"alarm_severity":"MAJOR","alarm_status":"DEVICE"}"#).unwrap();

        assert_eq!(metadata.alarm_severity, AlarmSeverity::Major);
        assert_eq!(metadata.alarm_status, AlarmStatus::Device);
    }
}
