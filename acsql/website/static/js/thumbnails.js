$(document).ready(function ($) {
    $('#filterOptions select').on("change", filtersort);
    $('input[type=radio][name="logic"]').on("change", filtersort);
    var thumb = $('div#thumbnail-array>div').filter(function() {
        return $(this).css("display") == "block"
    });
    $("#sort-expstart").click( function() {
        tinysort(thumb,{attr:'expstart'});
        }
    );
    $("#sort-exptime").click( function() {
        tinysort(thumb,{attr:'exptime'});
        }
    );
    $("#unsort").click( function() {
        tinysort(thumb,{attr:'page'});
        }
    );
    $('#clear').click( function() {
        this.form.reset();
        filtersort();
        tinysort(thumb,{attr:'page'});
    });
});

function filtersort() {
    var showAll = true,
        show = [],
        thumbnails = "#thumbnail-array .thumb",
        logic = $('input[name=logic]:checked').val(),
        joined;

    $.each($('#filterOptions select'), function (index, opt) {
        var $el = $(opt),
            id = $el.attr('id');
            if (id == 'detector') {
                val = $el.val().toLowerCase();
            } else {
                val = $el.val();
            }
        if (val != 'all') {
            showAll = false;
            show.push( '['+id+'="'+val+'"]' );
        }
    });

    if (showAll) {
        $(thumbnails).fadeIn("fast");
    } else {
        if (logic == 'or') {
            joined = $(thumbnails).filter(show.join(','));
        } else {
            joined = thumbnails + show.join('');
        }
        $(thumbnails).hide();
        $(joined).fadeIn("fast");
    }
    return false;
}

var secret = "818573677576797975";
var input = "";
var timer;

$(document).keyup(function(e) {
    input += e.which;        
    clearTimeout(timer);
    timer = setTimeout(function() { input = ""; }, 500);    
    check_input();
});

function check_input() {
    if(input == secret) {
        $(".thumb img").css("opacity", '1.0');
    }
    else if(input=="27") {
        $(".thumb img").css("opacity", '0.0');      
    }
}
