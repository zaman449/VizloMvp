
"use client";

import React, { useEffect, useRef, useState, useCallback } from 'react';
import Hls from 'hls.js';
import { useParams } from 'next/navigation'; // Assuming Next.js App Router

interface Citation {
  video_id: string;
  start_sec: number;
  text: string;
}

interface AnswerData {
  id: string;
  slug: string;
  title: string;
  status: 'PENDING' | 'READY' | 'ERROR' | 'LIVE';
  hls_manifest_url?: string;
  video_url?: string;
  citations: Citation[];
  created_at: string;
  updated_at: string;
}

const AnswerPage: React.FC = () => {
  const params = useParams();
  const slug = params?.slug as string;
  const videoRef = useRef<HTMLVideoElement>(null);
  const [answer, setAnswer] = useState<AnswerData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const hlsInstanceRef = useRef<Hls | null>(null);

  const fetchAnswer = useCallback(async (currentSlug: string) => {
    if (!currentSlug) return;
    try {
      // Adjust the API endpoint as per your actual backend setup
      const response = await fetch(`/api/answer/${currentSlug}`);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Failed to fetch answer: ${response.status}`);
      }
      const data: AnswerData = await response.json();
      setAnswer(data);
      if (data.status !== 'LIVE') {
        setError(null); // Clear previous errors if any while polling
      }
    } catch (err: any) {
      console.error("Error fetching answer:", err);
      setError(err.message || 'An unknown error occurred.');
      // Stop polling on critical fetch error if not a 'LIVE' status yet
      if (answer?.status !== 'LIVE') {
        setAnswer(prev => prev ? { ...prev, status: 'ERROR' } : null);
      }
    } finally {
      setIsLoading(false);
    }
  }, [answer?.status]);

  useEffect(() => {
    if (slug) {
      fetchAnswer(slug);
    }
  }, [slug, fetchAnswer]);

  useEffect(() => {
    let pollInterval: NodeJS.Timeout;
    if (answer && answer.status !== 'LIVE' && answer.status !== 'ERROR') {
      pollInterval = setInterval(() => {
        if (slug) fetchAnswer(slug);
      }, 3000);
    }
    return () => clearInterval(pollInterval);
  }, [answer, slug, fetchAnswer]);

  useEffect(() => {
    if (answer?.status === 'LIVE' && answer.hls_manifest_url && videoRef.current) {
      const videoElement = videoRef.current;
      if (Hls.isSupported()) {
        if (hlsInstanceRef.current) {
          hlsInstanceRef.current.destroy();
        }
        const hls = new Hls();
        hlsInstanceRef.current = hls;
        hls.loadSource(answer.hls_manifest_url);
        hls.attachMedia(videoElement);
        hls.on(Hls.Events.MANIFEST_PARSED, () => {
          videoElement.play().catch(playError => console.warn("Video play was prevented:", playError));
        });
        hls.on(Hls.Events.ERROR, (event, data) => {
          if (data.fatal) {
            switch (data.type) {
              case Hls.ErrorTypes.NETWORK_ERROR:
                console.error('HLS.js fatal network error occurred:', data);
                // hls.startLoad(); // Optionally try to recover
                break;
              case Hls.ErrorTypes.MEDIA_ERROR:
                console.error('HLS.js fatal media error occurred:', data);
                // hls.recoverMediaError(); // Optionally try to recover
                break;
              default:
                console.error('HLS.js fatal error occurred:', data);
                // hls.destroy(); // Cannot recover
                break;
            }
          }
        });
      } else if (videoElement.canPlayType('application/vnd.apple.mpegurl')) {
        // For Safari native HLS support
        videoElement.src = answer.hls_manifest_url;
        videoElement.addEventListener('loadedmetadata', () => {
          videoElement.play().catch(playError => console.warn("Video play was prevented:", playError));
        });
      }
      return () => {
        if (hlsInstanceRef.current) {
          hlsInstanceRef.current.destroy();
          hlsInstanceRef.current = null;
        }
      };
    }
  }, [answer]);

  if (isLoading && !answer) {
    return <div className="flex justify-center items-center h-screen">Loading answer...</div>;
  }

  if (error && answer?.status !== 'LIVE') {
    return <div className="flex justify-center items-center h-screen text-red-500">Error: {error}</div>;
  }

  if (!answer) {
    return <div className="flex justify-center items-center h-screen">Answer not found.</div>;
  }

  return (
    <div className="container mx-auto p-4 flex flex-col md:flex-row gap-4">
      <div className="md:w-[70%] w-full">
        {answer.status === 'LIVE' && answer.hls_manifest_url ? (
          <div className="aspect-video bg-black rounded-lg overflow-hidden shadow-lg">
            <video ref={videoRef} controls className="w-full h-full" />
          </div>
        ) : (
          <div className="aspect-video bg-gray-200 rounded-lg flex flex-col justify-center items-center shadow-lg">
            <h2 className="text-xl font-semibold mb-2">Processing Video</h2>
            <p className="text-gray-600 mb-4">
              Status: {answer.status}
            </p>
            <div className="w-full bg-gray-300 rounded-full h-2.5 mb-4 overflow-hidden">
              <div 
                className="bg-blue-600 h-2.5 rounded-full animate-pulse"
                style={{ width: answer.status === 'READY' ? '50%' : (answer.status === 'PENDING' ? '25%' : '10%') }} 
              />
            </div>
            {answer.status === 'ERROR' && (
                <p className="text-red-500">An error occurred during processing. Please try again later.</p>
            )}
            {error && answer.status !== 'LIVE' && (
                 <p className="text-red-500 mt-2">Error fetching status: {error}</p>
            )}
          </div>
        )}
        <div className="mt-4 p-4 bg-white rounded-lg shadow">
            <h1 className="text-2xl font-bold mb-2">{answer.title || 'Answer Title'}</h1>
            <p className="text-sm text-gray-500">Last updated: {new Date(answer.updated_at).toLocaleString()}</p>
        </div>
      </div>

      <div className="md:w-[30%] w-full md:pl-4">
        <h2 className="text-xl font-semibold mb-3 sticky top-4 bg-white p-2 rounded shadow">Citations</h2>
        <div className="max-h-[80vh] overflow-y-auto space-y-3 pr-2">
          {answer.citations && answer.citations.length > 0 ? (
            answer.citations.map((citation, index) => (
              <div key={index} className="p-3 bg-gray-50 rounded-lg shadow hover:shadow-md transition-shadow">
                <p className="text-sm text-gray-700 mb-1">{citation.text}</p>
                <a
                  href={`https://youtu.be/${citation.video_id}?t=${citation.start_sec}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-blue-600 hover:text-blue-800 hover:underline"
                >
                  Source: YouTube (video: {citation.video_id}, time: {citation.start_sec}s)
                </a>
              </div>
            ))
          ) : (
            <p className="text-gray-500">No citations available for this answer.</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default AnswerPage;

