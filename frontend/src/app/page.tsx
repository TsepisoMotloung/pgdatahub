'use client';

import { useState } from 'react';
import FileUploader from '@/components/FileUploader';
import ProcessForm from '@/components/ProcessForm';
import ResultsDisplay from '@/components/ResultsDisplay';

export default function Home() {
  const [uploadData, setUploadData] = useState<{
    import_id: string;
    suggested_table: string;
    available_sheets: string[];
  } | null>(null);
  const [results, setResults] = useState<any>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const handleUploadSuccess = (data: any) => {
    setUploadData(data);
    setResults(null);
  };

  const handleProcessStart = () => {
    setIsProcessing(true);
  };

  const handleProcessComplete = (data: any) => {
    setResults(data);
    setIsProcessing(false);
  };

  const handleReset = () => {
    setUploadData(null);
    setResults(null);
  };

  return (
    <main className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 py-12 px-4">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">
            PostgreSQL Data Hub
          </h1>
          <p className="text-xl text-gray-600">
            Upload and process Excel data into PostgreSQL
          </p>
        </div>

        <div className="grid grid-cols-1 gap-8">
          {/* Step 1: Upload */}
          <div className="bg-white rounded-lg shadow-md p-8">
            <div className="flex items-center mb-6">
              <div className="flex items-center justify-center w-10 h-10 bg-indigo-600 text-white rounded-full font-bold">
                1
              </div>
              <h2 className="text-2xl font-semibold text-gray-900 ml-4">
                Upload ZIP File
              </h2>
            </div>
            <FileUploader
              onSuccess={handleUploadSuccess}
              disabled={isProcessing}
            />
          </div>

          {/* Step 2: Configure & Process */}
          {uploadData && (
            <div className="bg-white rounded-lg shadow-md p-8">
              <div className="flex items-center mb-6">
                <div className="flex items-center justify-center w-10 h-10 bg-indigo-600 text-white rounded-full font-bold">
                  2
                </div>
                <h2 className="text-2xl font-semibold text-gray-900 ml-4">
                  Configure & Process
                </h2>
              </div>
              <ProcessForm
                uploadData={uploadData}
                onProcessStart={handleProcessStart}
                onProcessComplete={handleProcessComplete}
                disabled={isProcessing}
              />
            </div>
          )}

          {/* Step 3: Results */}
          {results && (
            <div className="bg-white rounded-lg shadow-md p-8">
              <div className="flex items-center mb-6">
                <div className="flex items-center justify-center w-10 h-10 bg-indigo-600 text-white rounded-full font-bold">
                  3
                </div>
                <h2 className="text-2xl font-semibold text-gray-900 ml-4">
                  Processing Results
                </h2>
              </div>
              <ResultsDisplay results={results} />
              <button
                onClick={handleReset}
                className="mt-6 w-full px-6 py-3 bg-indigo-600 text-white font-semibold rounded-lg hover:bg-indigo-700 transition"
              >
                Start Over
              </button>
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
