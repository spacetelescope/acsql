//Changes active class depending on page
jQuery(function() {
    var a, b, c, d, e;
    d = $(".navbar .nav a"), e = [];
    for (b = 0, c = d.length; b < c; b++)
        a = d[b], e.push(function(a) {
			if (window.location.pathname === a.pathname)
				return $(a).parent().toggleClass("active")
        }(a));
    return e
});
