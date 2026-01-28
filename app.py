"""NPS分析Web应用"""
import os
from flask import Flask, render_template, request, jsonify, Response, send_file
from werkzeug.utils import secure_filename
from nps_analyzer import NPSAnalyzer

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# 全局分析器实例
analyzer: NPSAnalyzer = None


def get_analyzer() -> NPSAnalyzer:
    global analyzer
    if analyzer is None:
        analyzer = NPSAnalyzer()
    return analyzer


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    global analyzer
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': '未选择文件'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': '未选择文件'})
    
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'success': False, 'message': '请上传Excel文件(.xlsx或.xls)'})
    
    # 获取NPS目标值
    nps_target = int(request.form.get('nps_target', 60))
    
    # 创建新的分析器实例
    analyzer = NPSAnalyzer(nps_target=nps_target)
    success, message = analyzer.load_excel(file)
    
    if success:
        return jsonify({
            'success': True, 
            'message': message,
            'overall_nps': round(analyzer.overall_nps, 2),
            'suppliers': analyzer.get_supplier_list()
        })
    else:
        return jsonify({'success': False, 'message': message})


@app.route('/api/set_target', methods=['POST'])
def set_target():
    global analyzer
    data = request.get_json()
    target = int(data.get('target', 60))
    
    if analyzer:
        analyzer.nps_target = target
        return jsonify({'success': True, 'target': target})
    return jsonify({'success': False, 'message': '请先上传数据文件'})


@app.route('/api/overall')
def get_overall():
    a = get_analyzer()
    if a.df is None:
        return jsonify({'success': False, 'message': '请先上传数据文件'})
    
    data = a.get_overall_analysis()
    return jsonify({
        'success': True,
        'data': data,
        'overall_nps': round(a.overall_nps, 2),
        'target': a.nps_target
    })


@app.route('/api/supplier/<int:supplier_id>')
def get_supplier(supplier_id: int):
    a = get_analyzer()
    if a.df is None:
        return jsonify({'success': False, 'message': '请先上传数据文件'})
    
    return jsonify({
        'success': True,
        'followup': a.get_followup_management(supplier_id),
        'date_dimension': a.get_date_dimension(supplier_id),
        'account_dimension': a.get_account_dimension(supplier_id),
        'follower_dimension': a.get_follower_dimension(supplier_id)
    })


@app.route('/download/overall')
def download_overall():
    a = get_analyzer()
    if a.df is None:
        return jsonify({'success': False, 'message': '请先上传数据文件'})
    
    data = a.get_overall_analysis()
    columns = ['排名', '供应商ID', '供应商名称', '订单数', '有效分母', '诋毁数', 
               '推荐分', '诋毁率', '推荐率', 'NPS', '未达目标', '对整体贡献', '负贡献']
    csv_content = a.to_csv(data, columns)
    
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=nps_overall_analysis.csv'}
    )


@app.route('/download/supplier/<int:supplier_id>/<dtype>')
def download_supplier(supplier_id: int, dtype: str):
    a = get_analyzer()
    if a.df is None:
        return jsonify({'success': False, 'message': '请先上传数据文件'})
    
    if dtype == 'followup':
        data = a.get_followup_management(supplier_id)
        columns = ['订单号', '优先级', '优先级说明', '追评类型', '分母V5', 
                   '是否诋毁', '有用户反馈', '推荐得分']
        filename = f'追评管理_{supplier_id}.csv'
    elif dtype == 'date':
        data = a.get_date_dimension(supplier_id)
        columns = ['日期', '订单数', '有效分母', '诋毁数', '推荐分', 
                   '当日NPS', '累计NPS', '是否进步']
        filename = f'日期维度_{supplier_id}.csv'
    elif dtype == 'account':
        data = a.get_account_dimension(supplier_id)
        columns = ['子账号UID', '子账号名称', '订单数', '有效分母', '诋毁数',
                   '推荐分', '诋毁率', '推荐率', 'NPS', '贡献度', '负贡献']
        filename = f'账号维度_{supplier_id}.csv'
    elif dtype == 'follower':
        data = a.get_follower_dimension(supplier_id)
        columns = ['跟进人ID', '跟进人姓名', '订单数', '有效分母', '诋毁数',
                   '推荐分', '诋毁率', '推荐率', 'NPS', '贡献度', '负贡献']
        filename = f'跟进人维度_{supplier_id}.csv'
    else:
        return jsonify({'success': False, 'message': '未知的下载类型'})
    
    csv_content = a.to_csv(data, columns)
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@app.route('/download/all')
def download_all():
    a = get_analyzer()
    if a.df is None:
        return jsonify({'success': False, 'message': '请先上传数据文件'})
    
    zip_buffer = a.generate_all_csvs()
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name='nps_analysis_all.zip'
    )


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)
