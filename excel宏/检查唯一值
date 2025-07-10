Sub 检查唯一值()
    Dim rng As Range, cell As Range
    Dim dict As Object: Set dict = CreateObject("Scripting.Dictionary")
    
    ' 选择列
    On Error Resume Next
    Set rng = Application.InputBox("请选择要检查的列（单列范围）", Type:=8)
    On Error GoTo 0
    If rng Is Nothing Then Exit Sub
    
    ' 删除旧表
    On Error Resume Next
    Application.DisplayAlerts = False
    Worksheets("唯一值检查").Delete
    Application.DisplayAlerts = True
    On Error GoTo 0
    
    ' 新表并文本格式
    Dim wsNew As Worksheet: Set wsNew = Worksheets.Add(After:=Worksheets(Worksheets.Count))
    wsNew.Name = "唯一值检查"
    wsNew.Cells.NumberFormat = "@"
    
    ' 源表与表头
    Dim srcSht As Worksheet: Set srcSht = rng.Worksheet
    Dim hdrRow As Long: hdrRow = rng.Cells(1).Row
    Dim lastCol As Long: lastCol = srcSht.Cells(hdrRow, srcSht.Columns.Count).End(xlToLeft).Column
    
    ' 复制表头
    Dim c As Long
    For c = 1 To lastCol
        wsNew.Cells(1, c).Value = srcSht.Cells(hdrRow, c).Value
    Next c
    
    ' 收集行号
    Dim key As String
    For Each cell In rng.Cells
        If Len(Trim(cell.Value)) > 0 Then
            key = LCase(CStr(cell.Value))
            If Not dict.Exists(key) Then Set dict(key) = New Collection
            dict(key).Add cell.Row
        End If
    Next cell
    
    ' 输出唯一行
    Dim outR As Long: outR = 2
    Dim v As Variant, rIdx As Variant
    For Each v In dict.Keys
        If dict(v).Count = 1 Then
            rIdx = dict(v)(1)
            For c = 1 To lastCol
                wsNew.Cells(outR, c).Value = srcSht.Cells(rIdx, c).Value
            Next c
            outR = outR + 1
        End If
    Next v
    
    MsgBox "已完成唯一值检查，结果在表：唯一值检查", vbInformation
End Sub

