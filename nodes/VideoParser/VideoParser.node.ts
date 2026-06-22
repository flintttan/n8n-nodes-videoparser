import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import axios from 'axios';

// Import btch-downloader functions
// The package is CJS with named exports; keep the require() form (project
// convention) to avoid aliasing every parser to a different function name.
const btchDownloader = require('btch-downloader');

type XiaohongshuResult = {
	noteId?: string;
	title?: string;
	desc?: string;
	keywords?: string;
	duration?: string | number;
	author?: {
		id?: string;
		nickname?: string;
		avatar?: string;
		profileUrl?: string;
	};
	engagement?: {
		likes?: number | string;
		comments?: number | string;
		collects?: number | string;
		shares?: number | string;
	};
	images?: string[];
	downloads?: { quality?: string; url?: string }[];
};

type XiaohongshuProfileResult = {
	status?: boolean;
	user?: {
		id?: string;
		redId?: string;
		nickname?: string;
		avatar?: string;
		profileUrl?: string;
		bio?: string;
		gender?: number;
		ipLocation?: string;
		verified?: boolean;
		verifyType?: number;
	};
	stats?: {
		followers?: number;
		followings?: number;
		likes?: number;
		notes?: number;
	};
	notes?: Array<{
		noteId?: string;
		title?: string;
		type?: string;
		cover?: string;
		likes?: number;
	}>;
	pagination?: {
		hasMore?: boolean;
		nextCursor?: string;
	};
};

const XIAOHONGSHU_PROFILE_REGEX = /xiaohongshu\.com\/user\/profile\/[\w]+/i;

export class VideoParser implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Video Parser',
		name: 'videoParser',
		icon: 'file:videoparser.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: '解析和下载抖音、快手、B站等视频平台的视频',
		defaults: {
			name: 'Video Parser',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{
				displayName: '视频链接',
				name: 'videoUrl',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'https://v.douyin.com/xxxxx/',
				description: '视频分享链接（支持抖音、快手、B站、TikTok等平台）',
			},
			{
				displayName: '平台',
				name: 'platform',
				type: 'options',
				options: [
					{
						name: '自动检测',
						value: 'auto',
					},
					{
						name: '抖音 (Douyin)',
						value: 'douyin',
					},
					{
						name: 'TikTok',
						value: 'tiktok',
					},
					{
						name: 'Instagram',
						value: 'instagram',
					},
					{
						name: 'Facebook',
						value: 'facebook',
					},
					{
						name: 'Twitter/X',
						value: 'twitter',
					},
					{
						name: 'YouTube',
						value: 'youtube',
					},
					{
						name: 'Kuaishou',
						value: 'kuaishou',
					},
					{
						name: 'Xiaohongshu',
						value: 'xiaohongshu',
					},
					{
						name: 'Xiaohongshu 博主主页 (Profile)',
						value: 'xiaohongshuProfile',
					},
				],
				default: 'auto',
				description: '选择视频平台（自动检测会根据URL自动识别）',
			},
			{
				displayName: '自动下载视频',
				name: 'downloadVideo',
				type: 'boolean',
				default: false,
				description: '是否自动下载视频文件（下载后作为二进制数据输出）',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const videoUrl = this.getNodeParameter('videoUrl', i) as string;
				const platform = this.getNodeParameter('platform', i) as string;
				const downloadVideo = this.getNodeParameter('downloadVideo', i) as boolean;

				if (!videoUrl) {
					throw new NodeOperationError(this.getNode(), '视频链接不能为空', { itemIndex: i });
				}

				// Parse video based on platform
				let videoInfo: any;

				if (platform === 'auto') {
					// Auto-detect platform from URL
					videoInfo = await detectAndParse(videoUrl);
				} else {
					// Use specified platform
					videoInfo = await parseByPlatform(videoUrl, platform);
				}

				if (!videoInfo) {
					throw new NodeOperationError(
						this.getNode(),
						`无法解析视频链接: ${videoUrl}`,
						{ itemIndex: i },
					);
				}

				const outputData: INodeExecutionData = {
					json: buildOutput(videoInfo, platform, videoUrl),
				};

				// Download media (videos or images) when requested
				if (downloadVideo) {
					const json = outputData.json as any;
					if (platform === 'xiaohongshuProfile') {
						// Profile endpoint doesn't yield downloadable media; nothing to fetch.
					} else if (json.contentType === 'video' && json.videoUrl) {
						try {
							const binaryData = await downloadToBinary(
								this,
								json.videoUrl as string,
								`video_${Date.now()}.mp4`,
								'video/mp4',
							);
							outputData.binary = { data: binaryData };
						} catch (error) {
							throw wrapDownloadError(this, i, error, '视频下载失败');
						}
					} else if (json.contentType === 'image' && Array.isArray(json.images) && json.images.length > 0) {
						try {
							const binaryDataObject: { [key: string]: any } = {};
							for (let imgIndex = 0; imgIndex < json.images.length; imgIndex++) {
								const imageUrl = json.images[imgIndex];
								const binaryData = await downloadToBinary(
									this,
									imageUrl,
									`image_${imgIndex + 1}.jpg`,
									'image/jpeg',
								);
								binaryDataObject[`image${imgIndex + 1}`] = binaryData;
							}
							outputData.binary = binaryDataObject;
						} catch (error) {
							throw wrapDownloadError(this, i, error, '图片下载失败');
						}
					}
				}

				returnData.push(outputData);
			} catch (error) {
				if (this.continueOnFail()) {
					const errorMessage = error instanceof Error ? error.message : String(error);
					returnData.push({
						json: {
							error: errorMessage,
						},
						pairedItem: {
							item: i,
						},
					});
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}

}

// Extract the inner payload from a btch-downloader response.
// Newer versions wrap the data in `result`; some platforms nest it deeper.
function unwrapPayload(videoInfo: any): any {
	return videoInfo?.result?.data || videoInfo?.result || videoInfo?.data || videoInfo;
}

function isXiaohongshuProfileResult(value: any): value is XiaohongshuProfileResult {
	return (
		value &&
		typeof value === 'object' &&
		(value.user !== undefined || value.stats !== undefined || value.notes !== undefined)
	);
}

function buildOutput(videoInfo: any, platform: string, videoUrl: string): Record<string, any> {
	const payload = unwrapPayload(videoInfo);

	// Xiaohongshu profile endpoint: skip the note-extraction path entirely.
	if (platform === 'xiaohongshuProfile' || isXiaohongshuProfileResult(payload)) {
		return {
			platform: 'xiaohongshuProfile',
			contentType: 'profile',
			profileUrl: videoUrl,
			user: payload?.user || {},
			stats: payload?.stats || {},
			notes: payload?.notes || [],
			pagination: payload?.pagination || {},
			rawData: videoInfo,
		};
	}

	const links = payload?.links || [];
	const downloads = payload?.downloads || [];
	const images = payload?.images || [];

	let extractedMediaUrl = '';
	let contentType = 'video';
	if (links.length > 0) {
		// Standard video platforms (Douyin, TikTok, etc.) use `links[].url`.
		extractedMediaUrl = links[0].url || '';
	} else if (downloads.length > 0) {
		// Newer Xiaohongshu responses put video URLs inside `downloads[]`.
		extractedMediaUrl = downloads[0].url || '';
	} else if (images.length > 0) {
		// Xiaohongshu image content.
		contentType = 'image';
		extractedMediaUrl = images[0];
	}

	const engagement = payload?.engagement || {};
	const toInt = (v: any): number => (typeof v === 'string' ? parseInt(v, 10) || 0 : v || 0);

	// Xiaohongshu 6.0.35 nests the author under `author.nickname`; fall back to
	// the legacy flat shape so older payloads still render the right name.
	const authorNickname =
		(payload as XiaohongshuResult)?.author?.nickname ||
		(payload as any)?.author ||
		(payload as any)?.username ||
		(payload as any)?.nickname ||
		'';

	return {
		platform,
		contentType,
		title:
			(payload as XiaohongshuResult)?.title ||
			(payload as any)?.title ||
			(payload as any)?.nickname ||
			'',
		author: authorNickname,
		authorId: (payload as XiaohongshuResult)?.author?.id || '',
		authorAvatar: (payload as XiaohongshuResult)?.author?.avatar || '',
		authorProfileUrl: (payload as XiaohongshuResult)?.author?.profileUrl || '',
		noteId: (payload as XiaohongshuResult)?.noteId || '',
		videoUrl: contentType === 'video' ? extractedMediaUrl : '',
		imageUrl: contentType === 'image' ? extractedMediaUrl : '',
		coverUrl: payload?.thumbnail || payload?.cover_url || payload?.coverUrl || '',
		duration: payload?.duration || 0,
		description: payload?.description || payload?.caption || payload?.desc || '',
		keywords: payload?.keywords || '',
		tags: payload?.tags || payload?.hashtags || [],
		stats: {
			likes: toInt(engagement.likes ?? payload?.likes ?? payload?.like_count ?? payload?.digg_count),
			comments: toInt(engagement.comments ?? payload?.comments ?? payload?.comment_count),
			shares: toInt(engagement.shares ?? payload?.shares ?? payload?.share_count),
			views: toInt(payload?.views ?? payload?.view_count ?? payload?.play_count),
			collects: toInt(engagement.collects ?? payload?.collects),
		},
		links,
		downloads,
		images,
		rawData: videoInfo,
	};
}

async function downloadToBinary(
	ctx: IExecuteFunctions,
	url: string,
	fileName: string,
	mimeType: string,
): Promise<any> {
	const response = await axios.get(url, {
		responseType: 'arraybuffer',
		timeout: 60000,
	});
	return await ctx.helpers.prepareBinaryData(
		Buffer.from(response.data),
		fileName,
		mimeType,
	);
}

function wrapDownloadError(
	ctx: IExecuteFunctions,
	itemIndex: number,
	error: unknown,
	prefix: string,
): NodeOperationError {
	const errorMessage = error instanceof Error ? error.message : String(error);
	return new NodeOperationError(ctx.getNode(), `${prefix}: ${errorMessage}`, { itemIndex });
}

async function detectAndParse(url: string): Promise<any> {
		// btch-downloader 6.0.35 renamed several parsers:
		//   tiktok -> ttdl, instagram -> igdl, facebook -> fbdown
		// and dropped bilibili entirely. Resolve through the unified parser map.
		const detected = detectPlatformFromUrl(url);
		if (!detected) {
			throw new Error('无法识别视频平台，请手动选择平台');
		}
		const parser = platformMap[detected];
		return await parser(url);
	}

// Map a user-facing platform option (the value stored in n8n workflows)
// to the corresponding parser exported by btch-downloader. The platform
// value stays stable for backward compatibility; the underlying function
// name may change between releases.
const platformMap: { [key: string]: (url: string) => Promise<any> } = {
		douyin: btchDownloader.douyin,
		tiktok: btchDownloader.ttdl,
		instagram: btchDownloader.igdl,
		facebook: btchDownloader.fbdown,
		twitter: btchDownloader.twitter,
		youtube: btchDownloader.youtube,
		kuaishou: btchDownloader.kuaishou,
		xiaohongshu: btchDownloader.xiaohongshu,
		xiaohongshuProfile: btchDownloader.xiaohongshuProfile,
	};

function detectPlatformFromUrl(url: string): string | null {
	if (url.includes('douyin.com') || url.includes('iesdouyin.com')) {
		return 'douyin';
	} else if (url.includes('tiktok.com')) {
		return 'tiktok';
	} else if (url.includes('instagram.com')) {
		return 'instagram';
	} else if (url.includes('facebook.com') || url.includes('fb.watch')) {
		return 'facebook';
	} else if (url.includes('twitter.com') || url.includes('x.com')) {
		return 'twitter';
	} else if (url.includes('youtube.com') || url.includes('youtu.be')) {
		return 'youtube';
	} else if (url.includes('kuaishou.com')) {
		return 'kuaishou';
	} else if (XIAOHONGSHU_PROFILE_REGEX.test(url)) {
		return 'xiaohongshuProfile';
	} else if (url.includes('xiaohongshu.com') || url.includes('xhslink.com')) {
		return 'xiaohongshu';
	}
	return null;
}

async function parseByPlatform(url: string, platform: string): Promise<any> {
		const parserFunc = platformMap[platform];
		if (!parserFunc) {
			throw new Error(`不支持的平台: ${platform}`);
		}

		return await parserFunc(url);
}
