package bebras_guard

var blockMsg map[bool]string = map[bool]string{true: "block", false: "-"}
var rateMsg map[bool]string = map[bool]string{true: "ratelimit", false: "-"}
var quickMsg map[bool]string = map[bool]string{true: "quick", false: "-"}
var staleMsg map[bool]string = map[bool]string{true: "stale", false: "-"}
