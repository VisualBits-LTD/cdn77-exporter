use percent_encoding::percent_decode_str;

/// Detect device type from User-Agent string.
/// Exact port of Python detect_device_type (exporter.py:1065-1157).
pub fn detect_device_type(user_agent: &str) -> &'static str {
    if user_agent.is_empty() {
        return "other";
    }

    // URL-decode and lowercase
    let decoded = percent_decode_str(user_agent)
        .decode_utf8_lossy()
        .to_string();
    let ua = decoded.trim().to_lowercase();

    // OTT platforms
    if ua.contains("appletv") || ua.contains("apple tv") || ua.contains("tvos") {
        return "apple_tv";
    }
    if ua.contains("roku") {
        return "roku";
    }
    // Python: "aft" in ua or "amazon" in ua and "fire" in ua
    // Due to operator precedence: "aft" in ua or ("amazon" in ua and "fire" in ua)
    if ua.contains("aft") || (ua.contains("amazon") && ua.contains("fire")) {
        return "firestick";
    }

    // Bots
    if ua.contains("bot")
        || ua.contains("crawler")
        || ua.contains("spider")
        || ua.contains("scraper")
        || ua.contains("headless")
        || ua.contains("frame capture")
        || ua.contains("yallbot")
    {
        return "bots";
    }

    // First-party apps with platform disambiguation
    if ua.contains("baron") || ua.contains("virtualrailfan") || ua.contains("virtual railfan") {
        if ua.contains("android") {
            return "android";
        }
        if ua.contains("ios") || ua.contains("iphone") || ua.contains("ipad") {
            return "ios";
        }
    }

    // iOS
    if ua.contains("iphone")
        || ua.contains("ipad")
        || ua.contains("cpu os")
        || ua.contains("applecoremedia")
    {
        return "ios";
    }

    // Android
    if ua.contains("android")
        || ua.contains("androidxmedia3")
        || ua.contains("exoplayer")
        || ua.contains("dalvik")
    {
        return "android";
    }

    // Streamology
    if ua.starts_with("lavf/")
        || ua.contains("gstreamer")
        || ua.contains("gst-launch")
        || ua.contains("libgst")
    {
        return "streamology";
    }

    // CFNetwork -> other
    if ua.contains("cfnetwork/") {
        return "other";
    }

    // Darwin -> web
    if ua.contains("darwin/") {
        return "web";
    }

    // Browsers
    if ua.contains("chrome")
        || ua.contains("firefox")
        || ua.contains("safari")
        || ua.contains("edge")
        || ua.contains("opera")
    {
        return "web";
    }

    "other"
}
