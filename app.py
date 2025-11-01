from flask import Flask, request, jsonify, render_template_string
from azure.storage.blob import BlobServiceClient
import os
import re
import json
import time
from collections import defaultdict
import string

app = Flask(__name__)

# Azure Blob Storage configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = 'documents'

# In-memory index (for production, use database like Cosmos DB)
inverted_index = defaultdict(list)
document_store = {}

# Stop words list
STOP_WORDS = set([
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
    'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
    'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she',
    'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their',
    'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me'
])

def preprocess_text(text):
    """Clean and preprocess text"""
    # Convert to lowercase
    text = text.lower()
    
    # Remove non-ASCII characters
    text = ''.join(char for char in text if ord(char) < 128)
    
    # Remove punctuation but keep spaces
    text = text.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    
    # Split into words
    words = text.split()
    
    # Remove stop words and short words
    words = [word for word in words if word not in STOP_WORDS and len(word) > 2]
    
    return words

def simple_stem(word):
    """Basic word stemming"""
    suffixes = ['ing', 'ed', 'es', 's', 'ly', 'tion', 'ness', 'ment']
    for suffix in suffixes:
        if word.endswith(suffix) and len(word) > len(suffix) + 2:
            return word[:-len(suffix)]
    return word

def build_index(doc_id, content, doc_name):
    """Build inverted index for a document"""
    lines = content.split('\n')
    
    # Store document metadata
    document_store[doc_id] = {
        'name': doc_name,
        'size': len(content),
        'lines': len(lines),
        'indexed_at': time.time()
    }
    
    for line_num, line in enumerate(lines, 1):
        if not line.strip():
            continue
            
        words = preprocess_text(line)
        
        for position, word in enumerate(words):
            # Apply stemming
            stemmed = simple_stem(word)
            
            # Store word location with context
            inverted_index[stemmed].append({
                'doc_id': doc_id,
                'line': line_num,
                'position': position,
                'original_line': line.strip()[:200]  # Limit line length
            })

def search_documents(query):
    """Search for documents matching query"""
    # Preprocess query
    query_words = preprocess_text(query)
    query_words = [simple_stem(word) for word in query_words]
    
    if not query_words:
        return []
    
    # Handle single word vs multi-word queries
    if len(query_words) == 1:
        # Single word search
        word = query_words[0]
        if word not in inverted_index:
            return []
        
        results = defaultdict(list)
        for occurrence in inverted_index[word]:
            doc_id = occurrence['doc_id']
            results[doc_id].append(occurrence)
    else:
        # Multi-word search: find documents with all words
        doc_matches = defaultdict(lambda: {'occurrences': [], 'word_count': set()})
        
        for word in query_words:
            if word in inverted_index:
                for occurrence in inverted_index[word]:
                    doc_id = occurrence['doc_id']
                    doc_matches[doc_id]['occurrences'].append(occurrence)
                    doc_matches[doc_id]['word_count'].add(word)
        
        # Filter to documents containing all query words
        results = {}
        for doc_id, data in doc_matches.items():
            if len(data['word_count']) == len(query_words):
                results[doc_id] = data['occurrences']
    
    # Format results
    formatted_results = []
    for doc_id, occurrences in results.items():
        # Get unique lines with highlights
        unique_lines = {}
        for occ in occurrences:
            line_num = occ['line']
            if line_num not in unique_lines:
                unique_lines[line_num] = occ['original_line']
        
        doc_info = document_store.get(doc_id, {})
        formatted_results.append({
            'document': doc_info.get('name', doc_id),
            'doc_id': doc_id,
            'matches': len(occurrences),
            'total_lines': doc_info.get('lines', 0),
            'lines': [{'line_num': k, 'content': v} for k, v in sorted(unique_lines.items())][:20]  # Limit to 20 lines
        })
    
    # Sort by number of matches
    formatted_results.sort(key=lambda x: x['matches'], reverse=True)
    
    return formatted_results

@app.route('/')
def home():
    """Home page with upload and search interface"""
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cloud Document Search Service</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); margin-bottom: 30px; }
            h1 { color: #667eea; margin-bottom: 10px; }
            .subtitle { color: #666; }
            
            .card { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 5px 15px rgba(0,0,0,0.2); margin-bottom: 20px; }
            .card h2 { color: #333; margin-bottom: 15px; font-size: 20px; }
            
            .upload-area { border: 3px dashed #667eea; border-radius: 8px; padding: 40px; text-align: center; background: #f8f9ff; cursor: pointer; transition: all 0.3s; }
            .upload-area:hover { background: #eef1ff; border-color: #764ba2; }
            .upload-area.dragover { background: #e0e7ff; border-color: #667eea; }
            .file-input { display: none; }
            .upload-icon { font-size: 48px; margin-bottom: 10px; }
            .upload-text { color: #667eea; font-size: 18px; font-weight: 600; }
            .upload-hint { color: #999; margin-top: 8px; }
            
            .search-box { display: flex; gap: 10px; margin-bottom: 20px; }
            .search-box input { flex: 1; padding: 15px; font-size: 16px; border: 2px solid #ddd; border-radius: 8px; transition: border 0.3s; }
            .search-box input:focus { outline: none; border-color: #667eea; }
            .search-box button { padding: 15px 35px; font-size: 16px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; transition: transform 0.2s; }
            .search-box button:hover { transform: translateY(-2px); }
            .search-box button:active { transform: translateY(0); }
            
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 20px; }
            .stat-card { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }
            .stat-number { font-size: 32px; font-weight: bold; margin-bottom: 5px; }
            .stat-label { font-size: 14px; opacity: 0.9; }
            
            .result { background: #f8f9ff; padding: 20px; border-radius: 8px; margin-bottom: 15px; border-left: 4px solid #667eea; }
            .result h3 { color: #667eea; margin-bottom: 10px; }
            .result-meta { color: #666; font-size: 14px; margin-bottom: 15px; }
            .line { background: white; padding: 12px; margin: 8px 0; border-radius: 6px; border-left: 3px solid #ddd; }
            .line-num { color: #667eea; font-weight: bold; margin-right: 10px; display: inline-block; min-width: 60px; }
            .line-content { color: #333; }
            
            .message { padding: 15px; border-radius: 8px; margin-bottom: 15px; display: none; }
            .message.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .message.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
            .message.info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
            
            .file-list { margin-top: 15px; }
            .file-item { background: #f8f9ff; padding: 10px 15px; border-radius: 6px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }
            .file-name { color: #333; font-weight: 500; }
            .file-size { color: #999; font-size: 14px; }
            
            .loading { display: none; text-align: center; padding: 20px; }
            .spinner { border: 4px solid #f3f3f3; border-top: 4px solid #667eea; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin: 0 auto; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📚 Cloud Document Search Service</h1>
                <p class="subtitle">Upload documents and search through them using intelligent text indexing</p>
            </div>
            
            <div id="message" class="message"></div>
            
            <div class="card">
                <h2>📤 Upload Documents</h2>
                <div class="upload-area" id="uploadArea">
                    <div class="upload-icon">📄</div>
                    <div class="upload-text">Click to upload or drag & drop text files</div>
                    <div class="upload-hint">Supports .txt files (Multiple files allowed)</div>
                    <input type="file" id="fileInput" class="file-input" accept=".txt" multiple>
                </div>
                <div class="file-list" id="fileList"></div>
            </div>
            
            <div class="card">
                <h2>🔍 Search Documents</h2>
                <div class="search-box">
                    <input type="text" id="query" placeholder="Enter search terms (e.g., 'cloud computing', 'machine learning')" />
                    <button onclick="search()">Search</button>
                </div>
                <div id="searchInfo" style="color: #666; font-size: 14px;"></div>
            </div>
            
            <div class="card">
                <h2>📊 Statistics</h2>
                <div class="stats" id="stats">
                    <div class="stat-card">
                        <div class="stat-number" id="docCount">0</div>
                        <div class="stat-label">Documents Indexed</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number" id="wordCount">0</div>
                        <div class="stat-label">Unique Words</div>
                    </div>
                </div>
            </div>
            
            <div id="loading" class="loading">
                <div class="spinner"></div>
                <p style="margin-top: 15px; color: #667eea;">Processing...</p>
            </div>
            
            <div id="results"></div>
        </div>
        
        <script>
            const uploadArea = document.getElementById('uploadArea');
            const fileInput = document.getElementById('fileInput');
            const fileList = document.getElementById('fileList');
            
            // Upload area click
            uploadArea.addEventListener('click', () => fileInput.click());
            
            // Drag and drop
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('dragover');
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('dragover');
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');
                handleFiles(e.dataTransfer.files);
            });
            
            fileInput.addEventListener('change', (e) => {
                handleFiles(e.target.files);
            });
            
            function handleFiles(files) {
                const fileArray = Array.from(files);
                fileList.innerHTML = '';
                
                fileArray.forEach(file => {
                    const item = document.createElement('div');
                    item.className = 'file-item';
                    item.innerHTML = `
                        <span class="file-name">📄 ${file.name}</span>
                        <span class="file-size">${(file.size / 1024).toFixed(2)} KB</span>
                    `;
                    fileList.appendChild(item);
                });
                
                uploadFiles(fileArray);
            }
            
            async function uploadFiles(files) {
                document.getElementById('loading').style.display = 'block';
                let successCount = 0;
                
                for (const file of files) {
                    const formData = new FormData();
                    formData.append('file', file);
                    
                    try {
                        const response = await fetch('/upload', {
                            method: 'POST',
                            body: formData
                        });
                        
                        const data = await response.json();
                        
                        if (response.ok) {
                            successCount++;
                        } else {
                            showMessage('error', `Error uploading ${file.name}: ${data.error}`);
                        }
                    } catch (error) {
                        showMessage('error', `Error uploading ${file.name}: ${error.message}`);
                    }
                }
                
                document.getElementById('loading').style.display = 'none';
                
                if (successCount > 0) {
                    showMessage('success', `Successfully uploaded and indexed ${successCount} document(s)!`);
                    loadStats();
                }
            }
            
            async function search() {
                const query = document.getElementById('query').value.trim();
                if (!query) {
                    showMessage('error', 'Please enter search terms');
                    return;
                }
                
                document.getElementById('loading').style.display = 'block';
                document.getElementById('results').innerHTML = '';
                
                try {
                    const response = await fetch('/search', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ query: query })
                    });
                    
                    const data = await response.json();
                    document.getElementById('loading').style.display = 'none';
                    
                    const searchInfo = document.getElementById('searchInfo');
                    const resultsDiv = document.getElementById('results');
                    
                    if (data.results.length === 0) {
                        searchInfo.innerHTML = `No results found for "${query}"`;
                        resultsDiv.innerHTML = '<div class="card"><p style="text-align: center; color: #999;">No matches found. Try different search terms.</p></div>';
                        return;
                    }
                    
                    searchInfo.innerHTML = `Found ${data.results.length} document(s) with matches in ${data.search_time}ms`;
                    
                    let html = '';
                    data.results.forEach(result => {
                        html += `
                            <div class="card result">
                                <h3>📄 ${result.document}</h3>
                                <div class="result-meta">
                                    <strong>${result.matches}</strong> matches found | 
                                    Document has ${result.total_lines} lines
                                </div>
                                <div>`;
                        
                        result.lines.forEach(line => {
                            html += `<div class="line">
                                <span class="line-num">Line ${line.line_num}:</span>
                                <span class="line-content">${line.content}</span>
                            </div>`;
                        });
                        
                        if (result.matches > result.lines.length) {
                            html += `<p style="color: #999; margin-top: 10px; font-style: italic;">
                                ... and ${result.matches - result.lines.length} more matches
                            </p>`;
                        }
                        
                        html += `</div></div>`;
                    });
                    
                    resultsDiv.innerHTML = html;
                } catch (error) {
                    document.getElementById('loading').style.display = 'none';
                    showMessage('error', 'Error performing search: ' + error.message);
                }
            }
            
            async function loadStats() {
                try {
                    const response = await fetch('/stats');
                    const data = await response.json();
                    
                    document.getElementById('docCount').textContent = data.total_documents;
                    document.getElementById('wordCount').textContent = data.total_unique_words.toLocaleString();
                } catch (error) {
                    console.error('Error loading stats:', error);
                }
            }
            
            function showMessage(type, text) {
                const msg = document.getElementById('message');
                msg.className = `message ${type}`;
                msg.textContent = text;
                msg.style.display = 'block';
                
                setTimeout(() => {
                    msg.style.display = 'none';
                }, 5000);
            }
            
            // Search on Enter key
            document.getElementById('query').addEventListener('keypress', (e) => {
                if (e.key === 'Enter') search();
            });
            
            // Load stats on page load
            loadStats();
        </script>
    </body>
    </html>
    '''
    return render_template_string(html)

@app.route('/search', methods=['POST'])
def search():
    """Search endpoint"""
    start_time = time.time()
    
    data = request.get_json()
    query = data.get('query', '')
    
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    
    results = search_documents(query)
    
    search_time = int((time.time() - start_time) * 1000)
    
    return jsonify({
        'query': query,
        'results': results,
        'search_time': search_time
    })

@app.route('/upload', methods=['POST'])
def upload():
    """Upload document and index it"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.txt'):
        return jsonify({'error': 'Only .txt files are supported'}), 400
    
    try:
        # Read file content
        content = file.read().decode('utf-8', errors='ignore')
        
        # Generate unique doc ID
        doc_id = f"{int(time.time())}_{file.filename}"
        
        # Upload to Azure Blob Storage if configured
        if AZURE_STORAGE_CONNECTION_STRING:
            try:
                blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
                container_client = blob_service_client.get_container_client(CONTAINER_NAME)
                blob_client = container_client.get_blob_client(doc_id)
                blob_client.upload_blob(content, overwrite=True)
            except Exception as e:
                print(f"Warning: Could not upload to blob storage: {e}")
        
        # Index the document
        build_index(doc_id, content, file.filename)
        
        return jsonify({
            'message': f'Successfully uploaded and indexed {file.filename}',
            'doc_id': doc_id,
            'size': len(content),
            'lines': len(content.split('\n'))
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/stats')
def stats():
    """Get index statistics"""
    return jsonify({
        'total_documents': len(document_store),
        'total_unique_words': len(inverted_index),
        'documents': [
            {
                'name': doc['name'],
                'size': doc['size'],
                'lines': doc['lines']
            }
            for doc in document_store.values()
        ]
    })

@app.route('/clear', methods=['POST'])
def clear_index():
    """Clear all indexed documents"""
    global inverted_index, document_store
    inverted_index = defaultdict(list)
    document_store = {}
    return jsonify({'message': 'Index cleared successfully'})

if __name__ == '__main__':
    # Initialize blob service if configured
    if AZURE_STORAGE_CONNECTION_STRING:
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)
            try:
                container_client.create_container()
            except:
                pass  # Container already exists
        except Exception as e:
            print(f"Warning: Could not initialize blob storage: {e}")
    
    # Run the app
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=True)