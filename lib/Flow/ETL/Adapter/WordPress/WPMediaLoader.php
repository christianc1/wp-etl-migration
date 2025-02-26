<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;

final class WPMediaLoader implements Loader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;
    private array $mediaDefaults = [
        'post_status' => 'inherit',
        'post_type' => 'attachment',
    ];

    public function __construct(
        private readonly array $config = []
    ) {
        $this->mediaDefaults = array_merge($this->mediaDefaults, $config['defaults'] ?? []);
    }

    public function create_normalizer(FlowContext $context): RowNormalizer
    {
        return new RowNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $normalizer = $this->create_normalizer($context);

        foreach ($rows as $row) {
            $this->insertMedia($row, $normalizer);
        }
    }

    /**
     * Insert multiple media items from a single row
     *
     * @param Row|array $row Row data
     * @param RowNormalizer|null $normalizer Optional normalizer
     * @return array Array of media IDs keyed by media type
     */
    public function insertMedia(Row | array $row, RowNormalizer | null $normalizer = null): array
    {
        // Normalize
        if ($row instanceof Row && $normalizer instanceof RowNormalizer) {
            $data = $normalizer->normalize($row);
        } else {
            $data = $row;
        }

        $media_groups = $this->groupMediaFields($data);
        if (empty($media_groups)) {
            return [];
        }

        $results = [];
        foreach ($media_groups as $media_key => $media_data) {
            if (empty($media_data['url'])) {
                continue;
            }

            try {
                $attachment_id = $this->sideloadSingleMedia($media_data);
                if ($attachment_id) {
                    $results["media.{$media_key}"] = $attachment_id;
                }
            } catch (RuntimeException $e) {
                // Log error but continue processing other media
                error_log("Failed to process media {$media_key}: " . $e->getMessage());
            }
        }

        return $results;
    }

    /**
     * Group media-related fields by their prefix
     * e.g., 'media.featured_image.url' and 'media.featured_image.alt_text' are grouped together
     */
    protected function groupMediaFields(array $data): array
    {
        $media_groups = [];

        foreach ($data as $key => $value) {
            if (!str_starts_with($key, 'media.')) {
                continue;
            }

            // Split the key into parts (e.g., ['media', 'featured_image', 'url'])
            $parts = explode('.', $key);
            if (count($parts) !== 3) {
                continue;
            }

            [, $media_key, $field_name] = $parts;
            $media_groups[$media_key][$field_name] = $value;
        }

        return $media_groups;
    }

    /**
     * Handle sideloading of a single media item
     */
    protected function sideloadSingleMedia(array $media_data): int
    {
        $url = $media_data['url'];
        $desc = $media_data['title'] ?? null;
        $post_parent = !empty($media_data['post_parent']) ? (int)$media_data['post_parent'] : 0;

        // Prepare post data for the attachment
        $post_data = array_filter([
            'post_title' => $media_data['title'] ?? '',
            'post_content' => $media_data['description'] ?? '',
            'post_excerpt' => $media_data['caption'] ?? '',
            'post_parent' => $post_parent,
        ]);

        // Download and create the attachment
        $attachment_id = $this->sideloadMedia($url, $post_parent, $desc, $post_data);

        if (is_wp_error($attachment_id)) {
            throw new RuntimeException("Failed to sideload media: " . $attachment_id->get_error_message());
        }

        // Update alt text if provided
        if (!empty($media_data['alt_text'])) {
            update_post_meta($attachment_id, '_wp_attachment_image_alt', $media_data['alt_text']);
        }

        // Process any meta fields
        foreach ($media_data as $key => $value) {
            if (str_starts_with($key, 'meta_')) {
                $meta_key = substr($key, 5);
                update_post_meta($attachment_id, $meta_key, $value);
            }
        }

        return $attachment_id;
    }

    protected function sideloadMedia(string $url, int $post_parent = 0, ?string $desc = null, array $post_data = []): int|\WP_Error
    {
        // Check for existing attachment by filename
        $filename = pathinfo($url, PATHINFO_FILENAME);

        // Try up to 3 variations of the filename (original, -1, -2)
        for ($i = 0; $i < 3; $i++) {
            $existing_attachment = get_posts(
                array(
                    'post_type'      => 'attachment',
                    'posts_per_page' => 1,
                    'title'          => $filename . ($i ? '-' . $i : ''),
                    'fields'         => 'ids',
                )
            );

            if (!empty($existing_attachment)) {
                $attachment_id = $existing_attachment[0];

                // Update the attachment with any new data if provided
                if (!empty($post_data)) {
                    $post_data['ID'] = $attachment_id;
                    wp_update_post($post_data);
                }

                return $attachment_id;
            }
        }

        // If no existing attachment found, proceed with sideloading
        if (!function_exists('media_sideload_image')) {
            require_once(ABSPATH . 'wp-admin/includes/media.php');
            require_once(ABSPATH . 'wp-admin/includes/file.php');
            require_once(ABSPATH . 'wp-admin/includes/image.php');
        }

        // Use media_sideload_image() with 'id' return type to get attachment ID
        $attachment_id = media_sideload_image($url, $post_parent, $desc, 'id');

        if (is_wp_error($attachment_id)) {
            return $attachment_id;
        }

        // Update the attachment post with any additional data
        if (!empty($post_data)) {
            $post_data['ID'] = $attachment_id;
            wp_update_post($post_data);
        }

        return $attachment_id;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $clone = clone $this;
        $clone->dateTimeFormat = $dateTimeFormat;
        return $clone;
    }

    public function withDefaults(array $defaults): self
    {
        $clone = clone $this;
        $clone->mediaDefaults = array_merge($this->mediaDefaults, $defaults);
        return $clone;
    }
}
