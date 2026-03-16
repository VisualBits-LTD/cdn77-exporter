use polars::prelude::*;

/// Build a Polars expression that detects device type from the `ua_lower` column.
/// Mirrors the Python detect_device_type cascade exactly.
pub fn device_type_expr() -> Expr {
    let ua = col("ua_lower");

    when(
        ua.clone().str().contains(lit("appletv"), false)
            .or(ua.clone().str().contains(lit("apple tv"), false))
            .or(ua.clone().str().contains(lit("tvos"), false)),
    )
    .then(lit("apple_tv"))
    .when(ua.clone().str().contains(lit("roku"), false))
    .then(lit("roku"))
    .when(
        ua.clone().str().contains(lit("aft"), false)
            .or(ua.clone().str().contains(lit("amazon"), false)
                .and(ua.clone().str().contains(lit("fire"), false))),
    )
    .then(lit("firestick"))
    .when(
        ua.clone().str().contains(lit("bot"), false)
            .or(ua.clone().str().contains(lit("crawler"), false))
            .or(ua.clone().str().contains(lit("spider"), false))
            .or(ua.clone().str().contains(lit("scraper"), false))
            .or(ua.clone().str().contains(lit("headless"), false))
            .or(ua.clone().str().contains(lit("frame capture"), false))
            .or(ua.clone().str().contains(lit("yallbot"), false)),
    )
    .then(lit("bots"))
    // First-party apps with android
    .when(
        (ua.clone().str().contains(lit("baron"), false)
            .or(ua.clone().str().contains(lit("virtualrailfan"), false))
            .or(ua.clone().str().contains(lit("virtual railfan"), false)))
            .and(ua.clone().str().contains(lit("android"), false)),
    )
    .then(lit("android"))
    // First-party apps with iOS
    .when(
        (ua.clone().str().contains(lit("baron"), false)
            .or(ua.clone().str().contains(lit("virtualrailfan"), false))
            .or(ua.clone().str().contains(lit("virtual railfan"), false)))
            .and(
                ua.clone().str().contains(lit("ios"), false)
                    .or(ua.clone().str().contains(lit("iphone"), false))
                    .or(ua.clone().str().contains(lit("ipad"), false)),
            ),
    )
    .then(lit("ios"))
    // iOS
    .when(
        ua.clone().str().contains(lit("iphone"), false)
            .or(ua.clone().str().contains(lit("ipad"), false))
            .or(ua.clone().str().contains(lit("cpu os"), false))
            .or(ua.clone().str().contains(lit("applecoremedia"), false)),
    )
    .then(lit("ios"))
    // Android
    .when(
        ua.clone().str().contains(lit("android"), false)
            .or(ua.clone().str().contains(lit("androidxmedia3"), false))
            .or(ua.clone().str().contains(lit("exoplayer"), false))
            .or(ua.clone().str().contains(lit("dalvik"), false)),
    )
    .then(lit("android"))
    // Streamology
    .when(
        ua.clone().str().starts_with(lit("lavf/"))
            .or(ua.clone().str().contains(lit("gstreamer"), false))
            .or(ua.clone().str().contains(lit("gst-launch"), false))
            .or(ua.clone().str().contains(lit("libgst"), false)),
    )
    .then(lit("streamology"))
    .when(ua.clone().str().contains(lit("cfnetwork/"), false))
    .then(lit("other"))
    .when(ua.clone().str().contains(lit("darwin/"), false))
    .then(lit("web"))
    .when(
        ua.clone().str().contains(lit("chrome"), false)
            .or(ua.clone().str().contains(lit("firefox"), false))
            .or(ua.clone().str().contains(lit("safari"), false))
            .or(ua.clone().str().contains(lit("edge"), false))
            .or(ua.str().contains(lit("opera"), false)),
    )
    .then(lit("web"))
    .otherwise(lit("other"))
}
