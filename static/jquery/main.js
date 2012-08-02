$(document).ready(function(){
	window.setInterval(refresh, 1000);
	$('#submit_task_form').submit(submit_task);
});

function submit_task()
{
	//alert("taskname:" + submit_task_form.taskname.value);
	//return false;

	var options = { 
	url:'submit_task',
	type:'POST',
	//dataType: 'json',
	success: function(result){
		alert(result);}
	};

	$('#submit_task_form').ajaxSubmit(options); 
	//为了不刷新页面,返回false
	return false;
}

function refresh(){
	$.getJSON('ajax?action=refresh&timestamp=' + new Date().getTime(), function(response){
		var tbody = $('#tasklist > tbody');
		tbody.empty();
		tbody.append('<tr valign="top"><th class="thead" align="left" width="10%">任务ID</th><th class="thead" align="left" width="10%">任务名称</th><th class="thead" align="left" width="10%">任务进度</th><th class="thead" align="left" width="30%">当前任务</th><th class="thead" align="left" width="10%">任务REF</th><th class="thead" align="left" width="10%">任务记录</th><th class="thead" align="left" width="20%">任务状态</th></tr>');
		
		$.each(response, function(idx, item) {
			var html = "";
			html += "<tr><td class='alt1'>" + item.id + "</td>";
			html += "<td class='alt1'>" + idx + "</td>";
			html += "<td class='alt1'>" + item.remain + "</td>";
			html += "<td class='alt1'>" + item.current + "</td>";
			html += "<td class='alt1'>" + item.ref + "</td>";		
			html += "<td class='alt1'>" + item.log_count + "条<a href='/ajax?type=showresult&name=" + idx + "'>任务日志</a></td>";
			
			html += "<td class='alt1' >";
			if (item.status == "pause"){
				html += "任务暂停\r<a href='/ajax?type=run&name=" + idx + "' >开始任务</a>\r";
			}
			if (item.status == "run"){
				html += "正在运行\r<a href='/ajax?type=pause&name=" + idx + "' >暂停任务</a>\r";
			}
			html += "<a href='/ajax?type=del&name=" + idx + "' >删除任务</a></td></tr>";

			$("#tasklist").last().append(html);
			//$('#tasklist tr:last').after(html);
		});
	});
}