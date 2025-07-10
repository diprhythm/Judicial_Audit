Sub 匹配判断()

    Dim ws As Worksheet
    Dim dataRange As Range, matchRange As Range
    Dim matchOption As Integer, outputOption As Integer
    Dim resultCol As Long
    Dim matchValues As Variant
    Dim rowCount As Long, totalMatches As Long
    Dim r As Long, i As Long
    Dim dataCell As String, matchVal As String
    Dim matchedList As String

    Set ws = ActiveSheet


    If MsgBox("请选择匹配模式：" & vbCrLf & _
              "Yes = 精确匹配" & vbCrLf & _
              "No  = 模糊匹配", _
              vbYesNo + vbQuestion, "匹配模式") = vbYes Then
        matchOption = 1
    Else
        matchOption = 2
    End If


    outputOption = Application.InputBox("请选择结果输出方式：" & vbCrLf & _
                                        "1 - 显示 匹配成功/匹配失败" & vbCrLf & _
                                        "2 - 显示匹配到的值（用;号分隔）", _
                                        "输出方式", Type:=1)
    If outputOption <> 1 And outputOption <> 2 Then
        MsgBox "无效的输出方式，操作已取消。", vbExclamation
        Exit Sub
    End If


    On Error Resume Next
    Set dataRange = Application.InputBox("请选择要被匹配的数据区域：", "数据区域", Type:=8)
    If dataRange Is Nothing Then
        MsgBox "未选择数据区域，操作已取消。", vbExclamation
        Exit Sub
    End If


    Set matchRange = Application.InputBox("请选择用于匹配的值区域：", "匹配值区域", Type:=8)
    If matchRange Is Nothing Then
        MsgBox "未选择匹配值区域，操作已取消。", vbExclamation
        Exit Sub
    End If
    On Error GoTo 0


    resultCol = dataRange.Columns(dataRange.Columns.Count).Column + 1
    ws.Columns(resultCol).Insert Shift:=xlToRight
    ws.Cells(dataRange.Row, resultCol).Value = "匹配结果"


    matchValues = matchRange.Value
    totalMatches = UBound(matchValues, 1)


    rowCount = dataRange.Rows.Count
    For r = 1 To rowCount
        matchedList = ""
        dataCell = CStr(dataRange.Cells(r, 1).Value)

        For i = 1 To totalMatches
            matchVal = CStr(matchValues(i, 1))
            If matchVal <> "" Then
                If matchOption = 1 Then

                    If dataCell = matchVal Then
                        matchedList = matchedList & matchVal & ";"
                    End If
                Else

                    If InStr(1, dataCell, matchVal, vbTextCompare) > 0 Then
                        matchedList = matchedList & matchVal & ";"
                    End If
                End If
            End If
        Next i


        With ws.Cells(dataRange.Row + r - 1, resultCol)
            If outputOption = 1 Then
                If matchedList <> "" Then
                    .Value = "匹配成功"
                Else
                    .Value = "匹配失败"
                End If
            Else
                If matchedList <> "" Then

                    If Right(matchedList, 1) = ";" Then
                        matchedList = Left(matchedList, Len(matchedList) - 1)
                    End If
                    .Value = matchedList
                Else
                    .Value = "无匹配"
                End If
            End If
        End With
    Next r

    MsgBox "匹配完成！", vbInformation

End Sub
