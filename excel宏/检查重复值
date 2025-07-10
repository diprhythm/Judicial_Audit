Sub 检查并复制重复值()
    Dim rng As Range, cell As Range
    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary")
    
    ' 让用户选择列范围
    On Error Resume Next
    Set rng = Application.InputBox("请选择要检查的列（单列范围）", Type:=8)
    On Error GoTo 0
    If rng Is Nothing Then Exit Sub
    
    ' 删除已有的“重复值检查”表（如果存在）
    On Error Resume Next
    Application.DisplayAlerts = False
    Worksheets("重复值检查").Delete
    Application.DisplayAlerts = True
    On Error GoTo 0
    
    ' 准备新表并设置文本格式以防科学计数法
    Dim wsNew As Worksheet
    Set wsNew = Worksheets.Add(After:=Worksheets(Worksheets.Count))
    wsNew.Name = "重复值检查"
    wsNew.Cells.NumberFormat = "@"
    
    ' 源工作表及表头行和最后使用列
    Dim srcSht As Worksheet
    Set srcSht = rng.Worksheet
    Dim headerRowIndex As Long
    headerRowIndex = rng.Cells(1).Row
    Dim lastCol As Long
    lastCol = srcSht.Cells(headerRowIndex, srcSht.Columns.Count).End(xlToLeft).Column
    
    ' 复制表头到新表（逐单元格赋值，避免剪贴板）
    Dim c As Long
    For c = 1 To lastCol
        wsNew.Cells(1, c).Value = srcSht.Cells(headerRowIndex, c).Value
    Next c
    
    ' 收集重复值所在行号（忽略空值，不区分大小写）
    Dim key As String
    For Each cell In rng.Cells
        If Len(Trim(cell.Value)) > 0 Then
            key = LCase(CStr(cell.Value))
            If Not dict.Exists(key) Then Set dict(key) = New Collection
            dict(key).Add cell.Row
        End If
    Next cell
    
    ' 将出现次数>1的行复制到新表，逐单元格赋值
    Dim outRow As Long: outRow = 2
    Dim v As Variant, rowIndex As Variant
    For Each v In dict.Keys
        If dict(v).Count > 1 Then
            For Each rowIndex In dict(v)
                For c = 1 To lastCol
                    wsNew.Cells(outRow, c).Value = srcSht.Cells(rowIndex, c).Value
                Next c
                outRow = outRow + 1
            Next rowIndex
        End If
    Next v
    
    MsgBox "已完成重复值检查，结果已复制到表）", vbInformation
End Sub

