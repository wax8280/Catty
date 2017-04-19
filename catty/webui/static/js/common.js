/**
 * Created by vincent on 17-4-12.
 */

$(document).ready(function () {
    $(".action").click(function () {
        var spider_name = $(this).data('spider_name');
        var action_type = $(this).data('action_type');
        var param = $(this).attr('param');
        $.get("./action?action_type=" + action_type + "&spider_name=" + spider_name + "&param=" + param, function (data, status) {
            UIkit.modal.alert("数据：" + data + "\n状态：" + status);
        });
    });

    $(".speed").click(function () {
        var spider_name = $(this).data('spider_name');
        UIkit.modal.prompt('Speed:', '', function (val) {
            val = parseInt(val)
            console.log(spider_name)
            if (isNaN(val)) {
                UIkit.modal.alert("Is not a digital!")
            }
            else {
                $.get("./action?action_type=set_speed&spider_name=" + spider_name + "&param=" + val, function (data, status) {
                    UIkit.modal.alert("数据：" + data + "\n状态：" + status);
                });
            }
        });
    });


    $(function () {

        var progressbar = $("#progressbar"),
            bar = progressbar.find('.uk-progress-bar'),
            settings = {

                action: '/upload',

                allow: '*.(py|pyc)',
                single: true,
                param: 'py_script',

                loadstart: function () {
                    bar.css("width", "0%").text("0%");
                    progressbar.removeClass("uk-hidden");
                },

                progress: function (percent) {
                    percent = Math.ceil(percent);
                    bar.css("width", percent + "%").text(percent + "%");
                },

                allcomplete: function (response) {
                    var r = $.parseJSON(response)
                    if (r['code'] == 0) {
                        bar.css("width", "100%").text("100%");

                        setTimeout(function () {
                            progressbar.addClass("uk-hidden");
                        }, 350);
                        location.reload();
                    }
                    else {
                        $(".spider-header").before('<div class="uk-alert" data-uk-alert> <a href="" class="uk-alert-close uk-close"></a><p>'
                            + response + '</p> </div>')
                    }

                },

            };
        var select = UIkit.uploadSelect($("#upload-select"), settings),
            drop = UIkit.uploadDrop($("#upload-drop"), settings);
    });
});