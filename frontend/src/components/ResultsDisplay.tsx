'use client';

import { ProcessResponse } from '@/lib/api';

interface ResultsDisplayProps {
  results: ProcessResponse;
}

export default function ResultsDisplay({ results }: ResultsDisplayProps) {
  const totalFiles = results.success.length + results.errors.length + results.skipped.length;
  const successRate = totalFiles > 0 ? Math.round((results.success.length / totalFiles) * 100) : 0;

  return (
    <div className="space-y-6">
      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-green-50 border border-green-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-green-700 font-medium">Successful</p>
              <p className="text-3xl font-bold text-green-600">{results.success.length}</p>
            </div>
            <svg className="w-10 h-10 text-green-400" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
            </svg>
          </div>
        </div>

        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-red-700 font-medium">Errors</p>
              <p className="text-3xl font-bold text-red-600">{results.errors.length}</p>
            </div>
            <svg className="w-10 h-10 text-red-400" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
            </svg>
          </div>
        </div>

        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-yellow-700 font-medium">Skipped</p>
              <p className="text-3xl font-bold text-yellow-600">{results.skipped.length}</p>
            </div>
            <svg className="w-10 h-10 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
            </svg>
          </div>
        </div>

        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-blue-700 font-medium">Success Rate</p>
              <p className="text-3xl font-bold text-blue-600">{successRate}%</p>
            </div>
            <svg className="w-10 h-10 text-blue-400" fill="currentColor" viewBox="0 0 20 20">
              <path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zM8 7a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zM14 4a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z" />
            </svg>
          </div>
        </div>
      </div>

      {/* Successful Files */}
      {results.success.length > 0 && (
        <div className="border border-green-200 rounded-lg overflow-hidden">
          <div className="bg-green-50 px-6 py-4 border-b border-green-200">
            <h3 className="text-lg font-semibold text-green-900">
              ✓ Successfully Processed ({results.success.length})
            </h3>
          </div>
          <div className="p-6">
            <ul className="space-y-2">
              {results.success.map((file, idx) => (
                <li key={idx} className="flex items-center text-green-700">
                  <svg className="w-5 h-5 mr-3 text-green-500" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                  {file}
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}

      {/* Errors */}
      {results.errors.length > 0 && (
        <div className="border border-red-200 rounded-lg overflow-hidden">
          <div className="bg-red-50 px-6 py-4 border-b border-red-200">
            <h3 className="text-lg font-semibold text-red-900">
              ✗ Errors ({results.errors.length})
            </h3>
          </div>
          <div className="p-6 space-y-4">
            {results.errors.map((item, idx) => (
              <div key={idx} className="bg-red-50 p-4 rounded-lg border border-red-100">
                <p className="font-medium text-red-900">{item.file}</p>
                <p className="text-red-700 text-sm mt-1">{item.error}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Skipped */}
      {results.skipped.length > 0 && (
        <div className="border border-yellow-200 rounded-lg overflow-hidden">
          <div className="bg-yellow-50 px-6 py-4 border-b border-yellow-200">
            <h3 className="text-lg font-semibold text-yellow-900">
              ⊘ Skipped ({results.skipped.length})
            </h3>
          </div>
          <div className="p-6 space-y-4">
            {results.skipped.map((item, idx) => (
              <div key={idx} className="bg-yellow-50 p-4 rounded-lg border border-yellow-100">
                <p className="font-medium text-yellow-900">{item.file}</p>
                <p className="text-yellow-700 text-sm mt-1">Reason: {item.reason}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
