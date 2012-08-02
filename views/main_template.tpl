<html>
<head>

<style type="text/css" id="vbulletin_css">
.tborder
{
	background: #FFFFFF;
	color: #13253C;
	border: 1px solid #8FA0B0;
}
.thead
{
	background: #D1D7DC repeat-x top left;
	color: #333333;
	font: 12px tahoma, verdana, geneva, lucida, 'lucida grande', arial, helvetica, sans-serif;
	border-bottom: 1px solid #E2E7F0;
}
.alt1
{
	background: #F5F5F5;
	color: #13253C;
	font: 12px 宋体,verdana, geneva, lucida, 'lucida grande', arial, helvetica, sans-serif;
	border-bottom: 1px solid #E2E7F0;line-height:20px; padding:5px;
}


div#maincontent {
	margin: 0;
	padding: 0;
	border: 0;
	width: 100%;
	background: transparent;
}
</style>


<script type="text/javascript" src="/static/jquery/jquery.js"></script>
<script type="text/javascript" src="/static/jquery/jquery.form.js"></script> 
<script type="text/javascript" src="/static/jquery/main.js"></script>
</head>

<body>

<div class="alt1">

<div width="70%" style="float: left">
<form id="submit_task_form">
任务名:<input name="task_name" type="text" /><input type="file" name="task_data" /><input type="submit" value="添加任务" />
</form>
</div>

<div width="29%" align="right">
<a href="/logout" align="right">logout</a>
</div>
<br>
</div>

<div id="maincontent" align="center">

<table id="tasklist" class="tborder" cellpadding="6" cellspacing="1" border="0" width="100%" align="center">

<tr valign="top">
<th class="thead" align="left" width="10%">任务ID</th>
<th class="thead" align="left" width="10%">任务名称</th>
<th class="thead" align="left" width="10%">任务进度</th>
<th class="thead" align="left" width="30%">当前任务</th>
<th class="thead" align="left" width="10%">任务REF</th>
<th class="thead" align="left" width="10%">任务记录</th>
<th class="thead" align="left" width="20%">任务状态</th>
</tr>

%for taskname, task in tasks.items():
	<tr>
	<td class="alt1" >{{task['id']}}</td>
	<td class="alt1" >{{taskname}}</td>
	<td class="alt1" >{{task['remain']}}</td>
	<td class="alt1" >{{task['current']}}</td>
	<td class="alt1" >{{task['ref']}}</td>
	<td class="alt1" >{{task['log_count']}}条<a href="/ajax?type=showresult&name={{taskname}}" >任务日志</a></td>

	<td class="alt1" >
	%if task['status'] == "pause":
	任务暂停
	<a href="/ajax?type=run&name={{taskname}}">开始任务</a>
	%else:
	正在运行
	<a href="/ajax?type=pause&name={{taskname}}">暂停任务</a>
	%end
	<a href="/ajax?type=del&name={{taskname}}">删除任务</a>
	</td>
	</tr>
%end

</table>
<div id="msg_div">
</div>

</div>

</body>
</html>