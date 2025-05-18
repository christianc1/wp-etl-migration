<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Loaders;

use Flow\ETL\Adapter\WordPress\Exception\{
	WPAdapterDatabaseException,
	WPAdapterDataException,
	WPAdapterMissingDataException
};
use Flow\ETL\Adapter\WordPress\Normalizers\{
    EntryNormalizer,
    RowNormalizer
};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

/**
 * WordPress Media Loader for ETL operations
 *
 * This loader handles the insertion and updating of WordPress media attachments during ETL processes.
 * It supports data normalization, sanitization, and proper error handling for media operations.
 * Can handle both local file paths and remote URLs for media import.
 * Supports multiple media entries like media.featured_image.url and media.social_image.url.
 *
 * @implements Loader
 */
final class WPMediaLoader implements Loader
{
    /**
     * @var string The format to use for datetime values
     */
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    /**
     * @var array<string, mixed> Default values for media attachments
     */
    private array $mediaDefaults = [
        'post_status' => 'inherit',
        'post_type' => 'attachment',
    ];

    /**
     * Constructor
     *
     * @param array<string, mixed> $config Configuration array for the loader
     */
    public function __construct(
        private readonly array $config = []
    ) {
        $this->mediaDefaults = array_merge($this->mediaDefaults, $config['defaults'] ?? []);
    }

    /**
     * Creates a row normalizer instance
     *
     * @param FlowContext $context The flow context
     * @return RowNormalizer The created normalizer
     */
    public function create_normalizer(FlowContext $context): RowNormalizer
    {
        return new RowNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    /**
     * Loads media from the provided rows
     *
     * @param Rows $rows The rows to process
     * @param FlowContext $context The flow context
     * @throws WPAdapterMissingDataException When no media is found to process
     * @return void
     */
    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            throw WPAdapterMissingDataException::noEntitiesFound('media', 'No media found to process');
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->insertMedia($row, $normalizer);
        }
    }

    /**
     * Insert multiple media items from a single row
     *
     * @param Row|array<string, mixed> $row Row data
     * @param RowNormalizer|null $normalizer Optional normalizer
     * @return array<string, int> Array of attachment IDs keyed by media key in format 'media.{key}.attachment_id'
     * @throws WPAdapterMissingDataException When no media fields are found
     */
    public function insertMedia(Row | array $row, ?RowNormalizer $normalizer = null): array
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        // Group media fields by their prefix
        $media_groups = $this->groupMediaFields($data);

        if (empty($media_groups)) {
            throw WPAdapterMissingDataException::noEntitiesFound('media_fields', 'No media fields found in the data', [
                'available_keys' => array_keys($data)
            ]);
        }

        $results = [];
        foreach ($media_groups as $media_key => $media_data) {
            // Skip entries without URL
            if (empty($media_data['url'])) {
                continue;
            }

            // Sanitize the media data
            $media_data = $this->sanitizeMediaData($media_key, $media_data);

            try {
                $attachment_id = $this->sideloadSingleMedia($media_data);
                if ($attachment_id) {
                    // For default media (key is 'default'), use 'media.attachment_id'
                    // For specific media (e.g., 'featured_image'), use 'media.featured_image.attachment_id'
                    $result_key = ($media_key === 'default') ? 'media.attachment_id' : "media.{$media_key}.attachment_id";
                    $results[$result_key] = $attachment_id;

                    // Add Attachment URL
                    $url = wp_get_attachment_url($attachment_id);
                    if ($url) {
                        $url_key = ($media_key === 'default') ? 'media.url' : "media.{$media_key}.url";
                        $results[$url_key] = $url;
                    }
                }
            } catch (RuntimeException $e) {
                // Log error but continue processing other media
                error_log("Failed to process media {$media_key}: " . $e->getMessage());
            }
        }

        if (empty($results)) {
            throw WPAdapterMissingDataException::noEntitiesFound('media', 'No media was successfully processed', [
                'media_groups' => array_keys($media_groups)
            ]);
        }

        return $results;
    }

    /**
     * Group media-related fields by their prefix
     * e.g., 'media.featured_image.url' and 'media.featured_image.alt_text' are grouped together
     *
     * @param array<string, mixed> $data The normalized data
     * @return array<string, array<string, mixed>> Grouped media fields
     */
    protected function groupMediaFields(array $data): array
    {
        $media_groups = [];

        // First, check for direct media entries (media.url, media.title, etc.)
        $direct_media = [];
        foreach ($data as $key => $value) {
            if (strpos($key, 'media.') === 0 && substr_count($key, '.') === 1) {
                $field_name = substr($key, 6); // Remove 'media.'
                $direct_media[$field_name] = $value;
            }
        }

        // If we have direct media with URL, add as 'default' entry
        if (!empty($direct_media['url'])) {
            $media_groups['default'] = $direct_media;
        }

        // Process structured media entries (media.featured_image.url, etc.)
        foreach ($data as $key => $value) {
            if (strpos($key, 'media.') === 0 && substr_count($key, '.') === 2) {
                // Split the key into parts (e.g., ['media', 'featured_image', 'url'])
                $parts = explode('.', $key, 3);
                if (count($parts) !== 3) {
                    continue;
                }

                [, $media_key, $field_name] = $parts;
                $media_groups[$media_key][$field_name] = $value;
            }
        }

        // Unset any large temporary variables we created
        unset($direct_media);

        return $media_groups;
    }

    /**
     * Sanitizes media data
     *
     * @param string $media_key The media key (e.g., 'featured_image')
     * @param array<string, mixed> $media_data The media data to sanitize
     * @return array<string, mixed> Sanitized media data
     */
    protected function sanitizeMediaData(string $media_key, array $media_data): array
    {
        $sanitized = [];

        // Sanitize URL
        if (isset($media_data['url'])) {
            $sanitized['url'] = esc_url_raw($media_data['url']);
        }

        // Sanitize text fields
        if (isset($media_data['title'])) {
            $sanitized['title'] = sanitize_text_field($media_data['title']);
        }

        if (isset($media_data['description'])) {
            $sanitized['description'] = wp_kses_post($media_data['description']);
        }

        if (isset($media_data['caption'])) {
            $sanitized['caption'] = sanitize_text_field($media_data['caption']);
        }

        if (isset($media_data['alt_text'])) {
            $sanitized['alt_text'] = sanitize_text_field($media_data['alt_text']);
        }

        // Sanitize post parent
        if (isset($media_data['post_parent'])) {
            $post_parent = absint($media_data['post_parent']);
            $sanitized['post_parent'] = get_post($post_parent) ? $post_parent : 0;
        }

        // Process any meta fields
        foreach ($media_data as $key => $value) {
            if (strpos($key, 'meta_') === 0) {
                $sanitized[$key] = $value; // Will be sanitized when updating meta
            }
        }

        return $sanitized;
    }

    /**
     * Handle sideloading of a single media item
     *
     * @param array<string, mixed> $media_data The media data
     * @return int The attachment ID
     * @throws WPAdapterDataException When required data is missing
     * @throws WPAdapterDatabaseException When media insertion fails
     */
    protected function sideloadSingleMedia(array $media_data): int
    {
        if (empty($media_data['url'])) {
            throw WPAdapterDataException::missingRequiredData('url', $media_data);
        }

        $url = $media_data['url'];
        $desc = $media_data['title'] ?? null; // Used as fallback title by media_handle_sideload
        $post_parent = !empty($media_data['post_parent']) ? (int)$media_data['post_parent'] : 0;

        // Prepare post data for the attachment (title, content, excerpt)
        // These will be used by media_handle_sideload or wp_update_post for existing attachments
        $attachment_post_fields = array_filter([
            'post_title' => $media_data['title'] ?? '',
            'post_content' => $media_data['description'] ?? '',
            'post_excerpt' => $media_data['caption'] ?? '',
            // 'post_parent' is handled separately by media_handle_sideload's direct argument
            // and in findExistingAttachment if we decide to update parent there.
            // For now, post_parent is primarily for new attachments via media_handle_sideload.
        ]);

        // Sideload media, $desc is used by media_handle_sideload as potential title.
        // $attachment_post_fields provides explicit title, content, excerpt.
        $attachment_id = $this->sideloadMedia($url, $post_parent, $desc, $attachment_post_fields);

        if (is_wp_error($attachment_id)) {
            // Ensure the WP_Error from sideloadMedia is converted to an exception
            throw WPAdapterDatabaseException::fromWPError($attachment_id, "Failed to sideload media from URL: {$url}");
        }

        // Update alt text if provided
        if (!empty($media_data['alt_text'])) {
            update_post_meta($attachment_id, '_wp_attachment_image_alt', $media_data['alt_text']);
        }

        // Process any custom meta fields
        foreach ($media_data as $key => $value) {
            if (strpos($key, 'meta_') === 0) {
                $meta_key = substr($key, 5);
                update_post_meta($attachment_id, $meta_key, $this->sanitizeMetaValue($value));
            }
        }

        return $attachment_id;
    }

    /**
     * Sideload media from URL, checking for existing attachments first.
     * This method orchestrates the finding of existing attachments or downloading and creating new ones.
     *
     * @param string $url The URL of the media to sideload
     * @param int $post_parent The parent post ID for new attachments
     * @param string|null $desc Description, often used as a fallback title for new attachments
     * @param array<string, mixed> $attachment_post_fields Contains 'post_title', 'post_content', 'post_excerpt' for the attachment
     * @return int|\WP_Error The attachment ID or WP_Error on failure
     */
    protected function sideloadMedia(string $url, int $post_parent = 0, ?string $desc = null, array $attachment_post_fields = []): int|\WP_Error
    {
        $url_path_part = strtok($url, '?');
        if ($url_path_part === false) {
            $url_path_part = $url;
        }
        $filename_base = pathinfo(wp_basename($url_path_part), PATHINFO_FILENAME);

        if (!empty($filename_base)) {
            $existing_attachment_id = $this->findExistingAttachment($filename_base, $attachment_post_fields);
            if (null !== $existing_attachment_id) {
                // update_post_meta($existing_attachment_id, '_source_url', $url);
                return $existing_attachment_id;
            }
        }

        $load_error = $this->ensureMediaFunctionsLoaded();
        if (is_wp_error($load_error)) {
            return $load_error;
        }

        $file_array = $this->downloadFileForSideloading($url);
        if (is_wp_error($file_array)) {
            return $file_array;
        }

        return $this->createAttachmentFromSideloadedFile(
            $file_array,
            $post_parent,
            $desc,
            $attachment_post_fields,
            $url
        );
    }

    /**
     * Finds an existing attachment by its filename base.
     *
     * @param string $filename_base The base name of the file (without extension).
     * @param array<string, mixed> $post_data_for_update Data to update the attachment post if found (e.g., title, content).
     * @return int|null Attachment ID if found, otherwise null.
     */
    private function findExistingAttachment(string $filename_base, array $post_data_for_update): ?int
    {
        for ($i = 0; $i < 3; $i++) {
            $search_title = $filename_base . ($i ? '-' . $i : '');
            $query_args = [
                'post_type'      => 'attachment',
                'posts_per_page' => 1,
                'post_status'    => 'inherit',
                'fields'         => 'ids',
            ];

            $query_args_title = $query_args;
            $query_args_title['title'] = $search_title;
            $existing_by_title = get_posts($query_args_title);

            if (!empty($existing_by_title)) {
                $attachment_id = $existing_by_title[0];
                if (!empty($post_data_for_update)) {
                    $update_data = $post_data_for_update;
                    $update_data['ID'] = $attachment_id;
                    wp_update_post($update_data);
                }
                return $attachment_id;
            }

            $query_args_name = $query_args;
            $query_args_name['name'] = sanitize_title($search_title);
            $existing_by_name = get_posts($query_args_name);

            if (!empty($existing_by_name)) {
                $attachment_id = $existing_by_name[0];
                if (!empty($post_data_for_update)) {
                    $update_data = $post_data_for_update;
                    $update_data['ID'] = $attachment_id;
                    wp_update_post($update_data);
                }
                return $attachment_id;
            }
        }
        return null;
    }

    /**
     * Ensures that necessary WordPress media functions are loaded.
     *
     * @return \WP_Error|null Null if successful, WP_Error if critical files cannot be loaded.
     */
    private function ensureMediaFunctionsLoaded(): ?\WP_Error
    {
        if (!function_exists('download_url') || !function_exists('media_handle_sideload')) {
            if (!defined('ABSPATH')) {
                return new \WP_Error('wp_not_loaded', 'WordPress ABSPATH not defined. Cannot load required media files.');
            }
            require_once ABSPATH . 'wp-admin/includes/media.php';
            require_once ABSPATH . 'wp-admin/includes/file.php';
            require_once ABSPATH . 'wp-admin/includes/image.php'; // For wp_generate_attachment_metadata etc.
        }
        return null;
    }

    /**
     * Downloads a file from a URL to a temporary location for sideloading.
     *
     * @param string $url The URL of the file to download.
     * @return array|\WP_Error An array with 'name' and 'tmp_name' on success, or WP_Error on failure.
     */
    private function downloadFileForSideloading(string $url): array|\WP_Error
    {
        $file_array = [];
        $file_name_from_url = wp_basename(strtok($url, '?') ?: $url);

        if (empty($file_name_from_url) || in_array($file_name_from_url, ['.', '..'], true)) {
            $url_path_for_ext = parse_url($url, PHP_URL_PATH) ?: '';
            $path_info_ext = pathinfo($url_path_for_ext, PATHINFO_EXTENSION);
            $file_name_from_url = 'sideloaded-file-' . substr(md5($url), 0, 8) . ($path_info_ext ? '.' . sanitize_file_name('.' . $path_info_ext) : '');
        }
        $file_array['name'] = sanitize_file_name($file_name_from_url);

        $temp_file_path = download_url($url);

        if (is_wp_error($temp_file_path)) {
            return $temp_file_path; // download_url() handles its own temp file cleanup on error.
        }
        $file_array['tmp_name'] = $temp_file_path;

        return $file_array;
    }

    /**
     * Creates a WordPress attachment from a sideloaded file.
     *
     * @param array $file_array Array containing 'tmp_name' and 'name' of the downloaded file.
     * @param int $post_parent The parent post ID.
     * @param string|null $desc A description for the media, used as a fallback title.
     * @param array<string, mixed> $attachment_post_fields Overrides for attachment post fields (e.g., 'post_title').
     * @param string $source_url The original URL of the media, stored in post meta.
     * @return int|\WP_Error The new attachment ID or WP_Error on failure.
     */
    private function createAttachmentFromSideloadedFile(
        array $file_array,
        int $post_parent,
        ?string $desc,
        array $attachment_post_fields,
        string $source_url
    ): int|\WP_Error {
        // $desc is a fallback for title if $attachment_post_fields['post_title'] is not set.
        $attachment_id = media_handle_sideload($file_array, $post_parent, $desc, $attachment_post_fields);

        if (is_wp_error($attachment_id)) {
            // media_handle_sideload() typically unlinks $file_array['tmp_name'] on error.
            return $attachment_id;
        }

        add_post_meta($attachment_id, '_source_url', $source_url);

        // $attachment_post_fields (title, content, excerpt) should have been applied by media_handle_sideload.
        return $attachment_id;
    }

    /**
     * Sanitizes a meta value based on its type
     *
     * @param mixed $value The meta value to sanitize
     * @return mixed Sanitized meta value
     */
    private function sanitizeMetaValue($value)
    {
        if (is_numeric($value)) {
            // If it's a numeric value, preserve its type (int or float)
            return is_float($value + 0) ? (float)$value : (int)$value;
        } elseif (is_string($value)) {
            // For strings, use appropriate sanitization
            if (strpos($value, '<') !== false && strpos($value, '>') !== false) {
                // If it looks like HTML, use wp_kses_post
                return wp_kses_post($value);
            } else {
                // Otherwise use standard text sanitization
                return sanitize_text_field($value);
            }
        } elseif (is_array($value)) {
            // For arrays, recursively sanitize each element
            return array_map([$this, 'sanitizeMetaValue'], $value);
        } elseif (is_bool($value) || is_null($value)) {
            // Preserve boolean and null values
            return $value;
        } else {
            // For any other type, return as is
            return $value;
        }
    }

    /**
     * Sets the datetime format for the loader
     *
     * @param string $dateTimeFormat The format to use (should be a valid PHP date format)
     * @return self New instance with the updated format
     */
    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    /**
     * Sets default values for media fields
     *
     * @param array<string, mixed> $defaults The default values to set
     * @return self New instance with the updated defaults
     */
    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->mediaDefaults = array_merge($this->mediaDefaults, $defaults);
        return $clone;
    }
}

